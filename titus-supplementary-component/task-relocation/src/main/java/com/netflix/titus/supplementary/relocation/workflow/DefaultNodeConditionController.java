package com.netflix.titus.supplementary.relocation.workflow;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.connector.Node;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.supplementary.relocation.util.RelocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DefaultNodeConditionController implements NodeConditionController {
    private static final Logger logger = LoggerFactory.getLogger(DefaultNodeConditionController.class);
    private final RelocationConfiguration configuration;
    private final NodeDataResolver nodeDataResolver;
    private final JobDataReplicator jobDataReplicator;
    private final ReadOnlyJobOperations jobOperations;
    private final JobManagementClient jobManagementClient;
    private final NodeConditionCtrlMetrics metrics;
    private final TitusRuntime titusRuntime;

    private static final CallMetadata CALL_METADATA = buildCallMetadata();
    private static final String CALLER_APP_ID = "titusrelocation";
    private static final String CALL_REASON = "This task was automatically terminated because the underlying host had issues.";

    private ScheduleReference scheduleReference;


    public DefaultNodeConditionController(RelocationConfiguration relocationConfiguration,
                                          NodeDataResolver nodeDataResolver,
                                          JobDataReplicator jobDataReplicator,
                                          ReadOnlyJobOperations jobOperations,
                                          JobManagementClient jobManagementClient,
                                          TitusRuntime titusRuntime) {
        this.configuration = relocationConfiguration;
        this.nodeDataResolver = nodeDataResolver;
        this.jobDataReplicator = jobDataReplicator;
        this.jobOperations = jobOperations;
        this.jobManagementClient = jobManagementClient;
        this.metrics = new NodeConditionCtrlMetrics(titusRuntime.getRegistry());
        this.titusRuntime = titusRuntime;
    }

    @Override
    public void activate() {
        logger.info("Activating DefaultNodeConditionController");
        ScheduleDescriptor nodeConditionControlLoopSchedulerDescriptor = ScheduleDescriptor.newBuilder()
                .withName("nodeConditionCtrl")
                .withDescription("Node Condition control loop")
                .withInitialDelay(Duration.ZERO)
                .withInterval(Duration.ofMillis(configuration.getNodeConditionControlLoopIntervalMs()))
                .withTimeout(Duration.ofMillis(configuration.getNodeConditionControlLoopTimeoutMs()))
                .withRetryerSupplier(() -> Retryers.exponentialBackoff(1, 5, TimeUnit.MINUTES))
                .build();
        this.scheduleReference = titusRuntime.getLocalScheduler().scheduleMono(nodeConditionControlLoopSchedulerDescriptor,
                this::handleNodesWithBadCondition, Schedulers.parallel());
    }

    @Override
    public void deactivate() {
        if (scheduleReference != null) {
            scheduleReference.cancel();
        }
    }

    @VisibleForTesting
    Mono<Void> handleNodesWithBadCondition(ExecutionContext executionContext) {
        int iterationCount = executionContext.getExecutionId().getTotal();
        logger.debug("Starting node condition controller iteration {} ...", iterationCount);
        if (hasStaleData()) {
            logger.info("Stale data. Skipping the node condition control loop iteration- {} ", iterationCount);
            return Mono.empty();
        }
        return handleNodesWithBadCondition();
    }

    private boolean hasStaleData() {
        long dataStaleness = getDataStalenessMs();
        boolean stale = dataStaleness > configuration.getDataStalenessThresholdMs();
        metrics.setStaleness(stale, dataStaleness);
        return stale;
    }

    private long getDataStalenessMs() {
        return Math.max(nodeDataResolver.getStalenessMs(), jobDataReplicator.getStalenessMs());
    }

    public Mono<Void> handleNodesWithBadCondition() {
        // Identify bad nodes from node resolver
        Map<String, Node> badConditionNodesById = nodeDataResolver.resolve().entrySet().stream().filter(nodeEntry -> nodeEntry.getValue().isInBadCondition())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Find jobs that are eligible for bad node condition treatment
        Set<String> eligibleJobIds = jobOperations.getJobs().stream()
                .filter(job -> {
                    JobDescriptor<?> jobDescriptor = job.getJobDescriptor();
                    Map<String, String> jobAttributes = jobDescriptor.getAttributes();
                    if (jobAttributes.containsKey(JobAttributes.JOB_PARAMETER_TERMINATE_ON_BAD_AGENT)) {
                        String value = jobAttributes.get(JobAttributes.JOB_PARAMETER_TERMINATE_ON_BAD_AGENT);
                        return Boolean.parseBoolean(value);
                    }
                    return false;
                })
                .map(Job::getId)
                .collect(Collectors.toSet());

        if (eligibleJobIds.isEmpty()) {
            logger.info("No jobs configured for task terminations on bad node conditions");
            metrics.setTasksTerminated(0);
            return Mono.empty();
        }

        // Find eligible tasks that are running on the bad condition nodes
        List<String> eligibleTaskIds = RelocationUtil.buildTasksFromNodesAndJobsFilter(badConditionNodesById, eligibleJobIds, jobOperations);
        if (configuration.isTaskTerminationOnBadNodeConditionEnabled()) {
            // Terminate tasks directly using JobManagementClient
            return Flux.fromIterable(eligibleTaskIds)
                    .delayElements(Duration.ofSeconds(1))
                    .flatMap(taskId -> {
                        return jobManagementClient.killTask(taskId, false, CALL_METADATA)
                                .doOnSuccess(v -> logger.info("Task {} terminated", taskId));
                    })
                    .doOnComplete(() -> metrics.setTasksTerminated(eligibleTaskIds.size()))
                    .doOnError(e -> logger.error("Exception terminating task ", e))
                    .then();
        } else {
            logger.info("Skipping {} task terminations on bad node conditions", eligibleTaskIds.size());
            metrics.setTasksTerminated(0);
        }
        return Mono.empty();
    }

    private static CallMetadata buildCallMetadata() {
        Caller caller = Caller.newBuilder().withCallerType(CallerType.Application).withId(CALLER_APP_ID).build();
        return CallMetadata.newBuilder().withCallers(Collections.singletonList(caller)).withCallReason(CALL_REASON).build();
    }
}
