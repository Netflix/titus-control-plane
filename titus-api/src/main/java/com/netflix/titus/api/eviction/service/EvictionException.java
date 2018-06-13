package com.netflix.titus.api.eviction.service;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.api.model.Tier;

public class EvictionException extends RuntimeException {

    public EvictionException(String message) {
        super(message);
    }

    public static EvictionException unexpectedReference(Reference reference) {
        return new EvictionException("Unexpected eviction target type: " + reference);
    }

    public static EvictionException taskNotFound(String taskId) {
        return new EvictionException("Task not found: " + taskId);
    }

    public static EvictionException capacityGroupNotFound(String capacityGroupName) {
        return new EvictionException("Capacity group not found: " + capacityGroupName);
    }

    public static EvictionException taskAlreadyStopped(Task task) {
        TaskState state = task.getStatus().getState();
        return state == TaskState.Finished
                ? new EvictionException("Task already finished: " + task.getId())
                : new EvictionException(String.format("Task terminating: taskId=%s, state=%s", task.getId(), state));
    }

    public static EvictionException taskNotScheduledYet(Task task) {
        return new EvictionException("Task not scheduled yet: " + task.getId());
    }

    public static EvictionException noAvailableGlobalQuota() {
        return new EvictionException("No global quota");
    }

    public static EvictionException noAvailableTierQuota(Tier tier) {
        return new EvictionException("No tier quota: " + tier);
    }

    public static EvictionException noAvailableCapacityGroupQuota(String capacityGroupName) {
        return new EvictionException("No capacity group quota: " + capacityGroupName);

    }

    public static EvictionException noAvailableJobQuota(Job<?> job, int desired, int started) {
        return new EvictionException(String.format("No job quota: jobId=%s, desired=%s, started=%s", job.getId(), desired, started));
    }
}
