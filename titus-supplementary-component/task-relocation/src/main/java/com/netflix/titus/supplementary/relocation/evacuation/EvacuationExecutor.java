package com.netflix.titus.supplementary.relocation.evacuation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataSnapshot;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental(detail = "Proof of concept", deadline = "09/01/2018")
class EvacuationExecutor {

    private static final Logger logger = LoggerFactory.getLogger(EvacuationExecutor.class);

    private final AgentDataReplicator agentDataReplicator;
    private final JobDataReplicator jobDataReplicator;
    private final EvictionDataReplicator evictionDataReplicator;

    private final EvictionServiceClient evictionServiceClient;

    private final EvacuationMetrics metrics;

    private AgentInstanceGroup instanceGroup;
    private AgentInstance instance;

    private final Map<String, String> pendingTaskTerminationsByJobId = new HashMap<>();

    EvacuationExecutor(AgentDataReplicator agentDataReplicator,
                       JobDataReplicator jobDataReplicator,
                       EvictionDataReplicator evictionDataReplicator,
                       EvictionServiceClient evictionServiceClient,
                       EvacuationMetrics metrics,
                       AgentInstanceGroup instanceGroup,
                       AgentInstance instance) {
        this.agentDataReplicator = agentDataReplicator;
        this.jobDataReplicator = jobDataReplicator;
        this.evictionDataReplicator = evictionDataReplicator;
        this.evictionServiceClient = evictionServiceClient;
        this.metrics = metrics;
        this.instanceGroup = instanceGroup;
        this.instance = instance;

        logger.info("Starting evacuation process of the agent instance: id={}, entity={}", instance.getId(), instance);
    }

    boolean shouldAbortEvacuation() {
        if (!refreshAgentData()) {
            logger.info("Agent instance not found. Terminating the evacuation process: {}", instance.getId());
            return true;
        }

        //TODO add quarantined replacement
        return false;
    }

    boolean evacuate() {
        removeTerminatedTasks();

        List<Task> runningTasks = getTasksRunningOnInstance(jobDataReplicator, instance);
        if (runningTasks.isEmpty()) {
            return true;
        }
        terminateRemainingTasks(runningTasks);
        return false;
    }

    private boolean refreshAgentData() {
        AgentInstanceGroup currentInstanceGroup = agentDataReplicator.getCurrent().findInstanceGroup(instanceGroup.getId()).orElse(null);
        if (currentInstanceGroup == null) {
            return false;
        }
        AgentInstance currentInstance = agentDataReplicator.getCurrent().findInstance(instance.getId()).orElse(null);
        if (currentInstance == null) {
            return false;
        }
        this.instanceGroup = currentInstanceGroup;
        this.instance = currentInstance;
        return true;
    }

    private void removeTerminatedTasks() {
        pendingTaskTerminationsByJobId.entrySet().removeIf(entry -> {
            String taskId = entry.getValue();
            return isTerminated(taskId);
        });
    }

    private void terminateRemainingTasks(List<Task> runningTasks) {
        EvictionDataSnapshot evictionData = evictionDataReplicator.getCurrent();

        long globalQuota = evictionData.getGlobalEvictionQuota().getQuota();
        long tierQuota = evictionData.getTierEvictionQuota(instanceGroup.getTier()).getQuota();
        long remainingSystemQuota = Math.min(globalQuota, tierQuota);

        Map<String, Long> remainingCapacityGroupQuotas = new HashMap<>();

        for (Task task : runningTasks) {
            if (task.getStatus().getState() == TaskState.KillInitiated) {
                continue;
            }
            if (remainingSystemQuota <= 0) {
                logger.info("Aborting termination process due to lack of the system quota (global or tier)");
                return;
            }
            String terminating = pendingTaskTerminationsByJobId.get(task.getJobId());
            if (terminating != null) {
                logger.info("Not terminating task, as another one is being terminated in the same job: candidate={}, terminating={}, jobId={}",
                        task.getId(), terminating, task.getJobId()
                );
                continue;
            }
            Job<?> job = jobDataReplicator.getCurrent().findJob(task.getJobId()).orElse(null);
            if (job == null) {
                continue;
            }
            if (hasJobQuota(job, task)) {
                EvictionQuota capacityGroupQuota = getCapacityGroupQuota(job);
                String quotaName = capacityGroupQuota.getReference().getName();
                long remaining = remainingCapacityGroupQuotas.computeIfAbsent(quotaName, c -> capacityGroupQuota.getQuota());
                if (remaining > 0) {
                    remainingSystemQuota--;
                    remainingCapacityGroupQuotas.put(quotaName, remaining - 1);
                    evictTask(task);
                } else {
                    logger.info("Aborting termination process due to lack of the capacity group quota: capacityGroup={}",
                            capacityGroupQuota.getReference().getName()
                    );
                }
            } else {
                logger.info("Aborting the task termination process due to job quota limits: candidate={}, jobId={}", task.getId(), task.getJobId());
            }
        }
    }

    private void evictTask(Task task) {
        Throwable error = evictionServiceClient.terminateTask(
                task.getId(),
                "Evacuating task from the quarantined agent instance: " + instance.getId()
        ).get();

        if (error != null) {
            metrics.evictionFailure(task, error);
            logger.info("Eviction service task termination error: taskId=={}", task.getId(), error);
            return;
        }
        metrics.evictionSucceeded(task);

        pendingTaskTerminationsByJobId.put(task.getJobId(), task.getId());
        logger.info("Terminating task: {}", task.getId());
    }

    private EvictionQuota getCapacityGroupQuota(Job<?> job) {
        EvictionDataSnapshot evictionData = evictionDataReplicator.getCurrent();
        return evictionData.
                findCapacityGroupEvictionQuota(job.getJobDescriptor().getCapacityGroup())
                .orElseGet(() ->
                        evictionData
                                .findCapacityGroupEvictionQuota("DEFAULT")
                                .orElseGet(() -> EvictionQuota.newBuilder()
                                        .withReference(Reference.capacityGroup(job.getJobDescriptor().getCapacityGroup()))
                                        .withQuota(0)
                                        .build()
                                )
                );
    }

    private boolean hasJobQuota(Job<?> job, Task task) {
        long desired = JobFunctions.getJobDesiredSize(job);
        long actual = jobDataReplicator.getCurrent().getTasks(job.getId()).stream()
                .filter(t -> t.getStatus().getState() == TaskState.Started || t.getId().equals(task.getId()))
                .count();
        return desired <= actual;
    }

    private boolean isTerminated(String taskId) {
        return jobDataReplicator.getCurrent().findTaskById(taskId)
                .map(task -> task.getRight().getStatus().getState() == TaskState.Finished)
                .orElse(true);
    }

    static List<Task> getTasksRunningOnInstance(JobDataReplicator jobDataReplicator, AgentInstance instance) {
        return jobDataReplicator.getCurrent().getTasks().stream()
                .filter(task -> isTaskRunningOnInstance(task, instance.getId()))
                .collect(Collectors.toList());
    }

    static boolean isAgentRunningTasks(JobDataReplicator jobDataReplicator, AgentInstance instance) {
        return jobDataReplicator.getCurrent().getTasks().stream()
                .anyMatch(task -> isTaskRunningOnInstance(task, instance.getId()));
    }

    static boolean isTaskRunningOnInstance(Task task, String instanceId) {
        TaskState state = task.getStatus().getState();
        if (state == TaskState.Finished) {
            return false;
        }

        String taskAgentId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID);
        return instanceId.equals(taskAgentId);
    }
}
