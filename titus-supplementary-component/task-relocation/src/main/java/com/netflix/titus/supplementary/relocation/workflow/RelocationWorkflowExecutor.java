/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.supplementary.relocation.workflow;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.IOExt;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.descheduler.DeschedulerService;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationArchiveStore;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import com.netflix.titus.supplementary.relocation.workflow.step.DeschedulerStep;
import com.netflix.titus.supplementary.relocation.workflow.step.MustBeRelocatedTaskCollectorStep;
import com.netflix.titus.supplementary.relocation.workflow.step.MustBeRelocatedTaskStoreUpdateStep;
import com.netflix.titus.supplementary.relocation.workflow.step.TaskEvictionResultStoreStep;
import com.netflix.titus.supplementary.relocation.workflow.step.TaskEvictionStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

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
                                      ReadOnlyAgentOperations agentOperations,
                                      JobDataReplicator jobDataReplicator,
                                      ReadOnlyJobOperations jobOperations,
                                      EvictionDataReplicator evictionDataReplicator,
                                      EvictionServiceClient evictionServiceClient,
                                      DeschedulerService deschedulerService,
                                      TaskRelocationStore activeStore,
                                      TaskRelocationArchiveStore archiveStore,
                                      TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.agentDataReplicator = agentDataReplicator;
        this.jobDataReplicator = jobDataReplicator;
        this.evictionDataReplicator = evictionDataReplicator;
        this.metrics = new EvacuationMetrics(titusRuntime);

        this.mustBeRelocatedTaskCollectorStep = new MustBeRelocatedTaskCollectorStep(agentOperations, jobOperations, titusRuntime);
        this.mustBeRelocatedTaskStoreUpdateStep = new MustBeRelocatedTaskStoreUpdateStep(activeStore);
        this.deschedulerStep = new DeschedulerStep(deschedulerService);
        this.taskEvictionStep = new TaskEvictionStep(evictionServiceClient, titusRuntime, Schedulers.parallel());
        this.taskEvictionResultStoreStep = new TaskEvictionResultStoreStep(archiveStore);

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

    private void nextRelocationStep(ExecutionContext executionContext) {
        long count = executionContext.getIteration().getTotal();
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
            return;
        }

        Map<String, TaskRelocationPlan> mustBeRelocatedTasks = mustBeRelocatedTaskCollectorStep.collectTasksThatMustBeRelocated();
        mustBeRelocatedTaskStoreUpdateStep.persistChangesInStore(mustBeRelocatedTasks);
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
