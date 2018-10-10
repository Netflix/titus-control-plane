package com.netflix.titus.supplementary.relocation.workflow;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.workflow.step.DeschedulerStep;
import com.netflix.titus.supplementary.relocation.workflow.step.MustBeRelocatedTaskCollectorStep;
import com.netflix.titus.supplementary.relocation.workflow.step.MustBeRelocatedTaskStoreUpdateStep;
import com.netflix.titus.supplementary.relocation.workflow.step.TaskEvictionResultStoreStep;
import com.netflix.titus.supplementary.relocation.workflow.step.TaskEvictionStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class RelocationWorkflowExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RelocationWorkflowExecutor.class);

    private final RelocationConfiguration configuration;

    private final AgentDataReplicator agentDataReplicator;
    private final JobDataReplicator jobDataReplicator;
    private final EvictionDataReplicator evictionDataReplicator;

    private final EvacuationMetrics metrics;
    private final ScheduleReference disposable;

    private final MustBeRelocatedTaskCollectorStep mustBeRelocatedTaskCollectorStep;
    private final DeschedulerStep deschedulerStep;
    private final MustBeRelocatedTaskStoreUpdateStep mustBeRelocatedTaskStoreUpdateStep;
    private final TaskEvictionResultStoreStep taskEvictionResultStoreStep;
    private final TaskEvictionStep taskEvictionStep;

    @Inject
    public RelocationWorkflowExecutor(RelocationConfiguration configuration,
                                      AgentDataReplicator agentDataReplicator,
                                      JobDataReplicator jobDataReplicator,
                                      EvictionDataReplicator evictionDataReplicator,
                                      TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.agentDataReplicator = agentDataReplicator;
        this.jobDataReplicator = jobDataReplicator;
        this.evictionDataReplicator = evictionDataReplicator;
        this.metrics = new EvacuationMetrics(titusRuntime);

        this.mustBeRelocatedTaskCollectorStep = new MustBeRelocatedTaskCollectorStep();
        this.mustBeRelocatedTaskStoreUpdateStep = new MustBeRelocatedTaskStoreUpdateStep();
        this.deschedulerStep = new DeschedulerStep();
        this.taskEvictionStep = new TaskEvictionStep();
        this.taskEvictionResultStoreStep = new TaskEvictionResultStoreStep();

        ScheduleDescriptor relocationScheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName("relocationWorkflow")
                .withDescription("Task relocation scheduler")
                .withInterval(Duration.ofMillis(configuration.getRelocationScheduleIntervalMs()))
                .withTimeout(Duration.ofMillis(configuration.getRelocationTimeoutMs()))
                .withRetryerSupplier(() -> Retryers.exponentialBackoff(1, 5, TimeUnit.MINUTES))
                .build();

        disposable = titusRuntime.getLocalScheduler().schedule(relocationScheduleDescriptor, this::nextRelocationStep, true);
    }

    @PreDestroy
    public void shutdown() {
        IOExt.closeSilently(disposable::close);
    }

    private void nextRelocationStep(long count) {
        logger.info("Starting task relocation iteration {}...", count);

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            doWork();
            logger.info("Task relocation iteration {} finished in {}sec", count, stopwatch.elapsed(TimeUnit.SECONDS));
        } catch (Exception e) {
            logger.error("Task relocation iteration {} failed after {}sec", count, stopwatch.elapsed(TimeUnit.SECONDS), e);
        }
    }

    private void doWork() {
        if (!checkNotStale()) {
            logger.info("Stale data. Skipping the task relocation iteration");
        }

        Map<String, TaskRelocationPlan> mustBeRelocatedTasks = mustBeRelocatedTaskCollectorStep.collectTasksThatMustBeRelocated();
        mustBeRelocatedTaskStoreUpdateStep.peristChangesInStore(mustBeRelocatedTasks);
        Map<String, TaskRelocationPlan> evictionPlan = deschedulerStep.deschedule(mustBeRelocatedTasks);
        Map<String, TaskRelocationStatus> evictionResult = taskEvictionStep.evict(evictionPlan);
        taskEvictionResultStoreStep.storeTaskEvictionResults(evictionPlan, evictionResult);
    }

    private boolean checkNotStale() {
        long dataStaleness = getDataStalenessMs();
        boolean staleness = dataStaleness > configuration.getDataStalenessThresholdMs();
        metrics.setStaleness(staleness, dataStaleness);
        return staleness;
    }

    private long getDataStalenessMs() {
        return Math.max(agentDataReplicator.getStalenessMs(), Math.max(jobDataReplicator.getStalenessMs(), evictionDataReplicator.getStalenessMs()));
    }
}
