package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.master.scheduler.TaskPlacementFailure.FailureKind;

import static com.netflix.titus.master.scheduler.SchedulerUtils.getTier;

class TaskPlacementFailureClassifier {

    private final CodeInvariants invariants;

    private final AtomicReference<Map<FailureKind, List<TaskPlacementFailure>>> failuresRef = new AtomicReference<>(Collections.emptyMap());

    TaskPlacementFailureClassifier(TitusRuntime titusRuntime) {
        this.invariants = titusRuntime.getCodeInvariants();
    }

    void update(SchedulingResult schedulingResult) {
        try {
            updateInternal(schedulingResult);
        } catch (Exception e) {
            invariants.unexpectedError("Unexpected error during task failure analysis", e);
        }
    }

    Map<FailureKind, List<TaskPlacementFailure>> getLastTaskPlacementFailures() {
        return failuresRef.get();
    }

    private void updateInternal(SchedulingResult schedulingResult) {
        Map<FailureKind, List<TaskPlacementFailure>> failures = new HashMap<>();

        for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry : schedulingResult.getFailures().entrySet()) {

            TaskRequest taskRequest = entry.getKey();
            List<TaskAssignmentResult> assignmentResults = entry.getValue();

            if (assignmentResults.isEmpty()) {
                invariants.inconsistent("Task placement failure with empty failure set: taskId=%s", taskRequest.getId());
                continue;
            }

            process(taskRequest, assignmentResults, failures);
        }

        this.failuresRef.set(failures);
    }

    private void process(TaskRequest taskRequest,
                         List<TaskAssignmentResult> assignmentResults,
                         Map<FailureKind, List<TaskPlacementFailure>> resultCollector) {
        Tier tier = getTier((QueuableTask) taskRequest);
        if (!hasActiveAgents(assignmentResults)) {
            resultCollector.computeIfAbsent(FailureKind.NoActiveAgents, k -> new ArrayList<>()).add(
                    new TaskPlacementFailure(taskRequest.getId(), FailureKind.NoActiveAgents, "", tier, -1)
            );
        } else if (isAboveCapacityLimit(assignmentResults)) {
            resultCollector.computeIfAbsent(FailureKind.NoActiveAgents, k -> new ArrayList<>()).add(
                    new TaskPlacementFailure(taskRequest.getId(), FailureKind.AboveCapacityLimit, "", tier, -1)
            );
        } else if (isAllAgentsFull(assignmentResults)) {
            resultCollector.computeIfAbsent(FailureKind.NoActiveAgents, k -> new ArrayList<>()).add(
                    new TaskPlacementFailure(taskRequest.getId(), FailureKind.AllAgentsFull, "", tier, -1)
            );
        } else if (isTooLargeToFit(assignmentResults)) {
            resultCollector.computeIfAbsent(FailureKind.NoActiveAgents, k -> new ArrayList<>()).add(
                    new TaskPlacementFailure(taskRequest.getId(), FailureKind.TooLargeToFit, "", tier, -1)
            );
        } else if (!processLaunchGuard(assignmentResults, resultCollector) && !processJobHardConstraints(assignmentResults, resultCollector)) {
            resultCollector.computeIfAbsent(FailureKind.NoActiveAgents, k -> new ArrayList<>()).add(
                    new TaskPlacementFailure(taskRequest.getId(), FailureKind.Unrecognized, "", tier, -1)
            );
        }
    }

    private boolean hasActiveAgents(Iterable<TaskAssignmentResult> assignmentResults) {
        return false;
    }

    private boolean isAboveCapacityLimit(List<TaskAssignmentResult> assignmentResults) {
        return false;
    }

    private boolean isAllAgentsFull(List<TaskAssignmentResult> assignmentResults) {
        return false;
    }

    private boolean isTooLargeToFit(List<TaskAssignmentResult> assignmentResults) {
        return false;
    }

    private boolean processLaunchGuard(List<TaskAssignmentResult> assignmentResults, Map<FailureKind, List<TaskPlacementFailure>> resultCollector) {
        return false;
    }

    private boolean processJobHardConstraints(List<TaskAssignmentResult> assignmentResults, Map<FailureKind, List<TaskPlacementFailure>> resultCollector) {
        return false;
    }
}
