package com.netflix.titus.master.scheduler.constraint;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;

public class V3UniqueHostConstraint implements ConstraintEvaluator {

    public static final String NAME = "UniqueAgentConstraint";

    private static final Result VALID = new Result(true, null);
    private static final Result INVALID = new Result(false, "Task from the same job already running on the agent");

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        // Ignore the constraint for non-V3 tasks.
        if (!(taskRequest instanceof V3QueueableTask)) {
            return VALID;
        }

        V3QueueableTask v3FenzoTask = (V3QueueableTask) taskRequest;
        String jobId = v3FenzoTask.getJob().getId();

        for (TaskRequest running : targetVM.getRunningTasks()) {
            if (check(jobId, running)) {
                return INVALID;
            }
        }

        for (TaskAssignmentResult assigned : targetVM.getTasksCurrentlyAssigned()) {
            if (check(jobId, assigned.getRequest())) {
                return INVALID;
            }
        }

        return VALID;
    }

    private boolean check(String jobId, TaskRequest running) {
        if (running instanceof V3QueueableTask) {
            V3QueueableTask v3FenzoRunningTask = (V3QueueableTask) running;
            return v3FenzoRunningTask.getJob().getId().equals(jobId);
        }
        return false;
    }
}
