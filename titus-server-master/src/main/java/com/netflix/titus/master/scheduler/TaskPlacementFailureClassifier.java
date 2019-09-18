/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.netflix.fenzo.AssignmentFailure;
import com.netflix.fenzo.ConstraintFailure;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMResource;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.master.scheduler.TaskPlacementFailure.FailureKind;
import com.netflix.titus.master.scheduler.constraint.AgentLaunchGuardConstraint;
import com.netflix.titus.master.scheduler.constraint.AgentManagementConstraint;
import com.netflix.titus.master.scheduler.constraint.IpAllocationConstraint;
import com.netflix.titus.master.scheduler.constraint.OpportunisticCpuConstraint;
import com.netflix.titus.master.scheduler.constraint.V3UniqueHostConstraint;
import com.netflix.titus.master.scheduler.constraint.V3ZoneBalancedHardConstraintEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TaskPlacementFailureClassifier<T extends TaskRequest> {

    private static final Logger logger = LoggerFactory.getLogger("TaskPlacementFailureLog");

    private static final String LOG_FORMAT = "        taskId=%s agentCount=%-5d";
    private static final String LOG_HARD_CONSTRAINT_FORMAT = LOG_FORMAT + " hardConstraints=%s";

    private static final long LOGGING_INTERVAL_MS = 30_000;

    private static final String EXCLUSIVE_HOST_CONSTRAINT_NAME = ExclusiveHostConstraint.class.getName();

    private final CodeInvariants invariants;

    private final AtomicReference<Map<FailureKind, Map<T, List<TaskPlacementFailure>>>> failuresRef = new AtomicReference<>(Collections.emptyMap());

    private final TokenBucket loggingTokenBucket = Limiters.createFixedIntervalTokenBucket(
            TaskPlacementFailureClassifier.class.getSimpleName(),
            1,
            1,
            1,
            LOGGING_INTERVAL_MS,
            TimeUnit.MILLISECONDS
    );

    TaskPlacementFailureClassifier(TitusRuntime titusRuntime) {
        this.invariants = titusRuntime.getCodeInvariants();
    }

    void update(SchedulingResult schedulingResult) {
        try {
            updateInternal(schedulingResult);
            writeToLog();
        } catch (Exception e) {
            invariants.unexpectedError("Unexpected error during task failure analysis", e);
        }
    }

    Map<FailureKind, Map<T, List<TaskPlacementFailure>>> getLastTaskPlacementFailures() {
        return failuresRef.get();
    }

    private void updateInternal(SchedulingResult schedulingResult) {
        Map<FailureKind, Map<T, List<TaskPlacementFailure>>> failures = new HashMap<>();

        for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry : schedulingResult.getFailures().entrySet()) {
            // assume all TaskRequests are of the correct type
            @SuppressWarnings("unchecked") T taskRequest = (T) entry.getKey();
            List<TaskAssignmentResult> assignmentResults = entry.getValue();

            if (assignmentResults.isEmpty()) {
                invariants.inconsistent("Task placement failure with empty failure set: taskId=%s", taskRequest.getId());
                continue;
            }

            process(taskRequest, assignmentResults, failures);
        }

        this.failuresRef.set(failures);
    }

    private void process(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                         Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {

        if (!processNoActiveAgent(taskRequest, assignmentResults, resultCollector)
                && !processAboveCapacityLimit(taskRequest, assignmentResults, resultCollector)
                && !processTooLargeToFit(taskRequest, assignmentResults, resultCollector)
                && !processLaunchGuard(taskRequest, assignmentResults, resultCollector)
                && !processJobHardConstraints(taskRequest, assignmentResults, resultCollector)
                && !processInUseIpAllocation(taskRequest, assignmentResults, resultCollector)
                && !processOpportunisticResources(taskRequest, assignmentResults, resultCollector)) {
            resultCollector.computeIfAbsent(FailureKind.Unrecognized, k -> new HashMap<>())
                    .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                    .add(new TaskPlacementFailure(taskRequest.getId(), FailureKind.Unrecognized, -1, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));
        }
    }

    private boolean processNoActiveAgent(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                         Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (canScheduleOnAgent(assignmentResult)) {
                return false;
            }
        }
        resultCollector.computeIfAbsent(FailureKind.NoActiveAgents, k -> new HashMap<>())
                .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                .add(new TaskPlacementFailure(taskRequest.getId(), FailureKind.NoActiveAgents, -1, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));
        return true;
    }

    private boolean processAboveCapacityLimit(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                              Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (!CollectionsExt.isNullOrEmpty(assignmentResult.getFailures())) {
                for (AssignmentFailure assignmentFailure : assignmentResult.getFailures()) {
                    if (assignmentFailure.getResource() == VMResource.ResAllocs) {
                        String message = assignmentFailure.getMessage();
                        if (message != null && message.contains("No guaranteed capacity left for queue")) {
                            resultCollector.computeIfAbsent(FailureKind.AboveCapacityLimit, k -> new HashMap<>())
                                    .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                                    .add(new TaskPlacementFailure(taskRequest.getId(), FailureKind.AboveCapacityLimit, -1, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean processTooLargeToFit(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                         Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        int count = 0;
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (canScheduleOnAgent(assignmentResult)) {
                if (CollectionsExt.isNullOrEmpty(assignmentResult.getFailures())) {
                    return false;
                }
                for (AssignmentFailure assignmentFailure : assignmentResult.getFailures()) {
                    if (!isOverAgentAvailableResources(assignmentFailure)) {
                        return false;
                    }
                }
                count++;
            }
        }
        resultCollector.computeIfAbsent(FailureKind.TooLargeToFit, k -> new HashMap<>())
                .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                .add(new TaskPlacementFailure(taskRequest.getId(), FailureKind.TooLargeToFit, count, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));

        return true;
    }

    private boolean processLaunchGuard(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                       Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        int count = 0;
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (isLaunchGuard(assignmentResult)) {
                count++;
            }
        }
        if (count == 0) {
            return false;
        }

        resultCollector.computeIfAbsent(FailureKind.LaunchGuard, k -> new HashMap<>())
                .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                .add(new TaskPlacementFailure(taskRequest.getId(), FailureKind.LaunchGuard, count, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));

        return true;
    }

    private boolean processJobHardConstraints(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                              Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        int count = 0;
        Set<String> hardConstraints = new HashSet<>();
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (isJobHardConstraint(assignmentResult, hardConstraints)) {
                count++;
            }
        }
        if (count == 0) {
            return false;
        }

        resultCollector.computeIfAbsent(FailureKind.JobHardConstraint, k -> new HashMap<>())
                .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                .add(new JobHardConstraintPlacementFailure(taskRequest.getId(), count, hardConstraints, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));

        return true;
    }

    private boolean processInUseIpAllocation(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                             Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        int count = 0;
        Optional<String> inUseTaskIdCollector = Optional.empty();
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (isInUseIpAllocation(assignmentResult)) {
                count++;
                if (!inUseTaskIdCollector.isPresent()) {
                    inUseTaskIdCollector = IpAllocationConstraint.getTaskIdFromIpAllocationInUseReason(assignmentResult.getConstraintFailure().getReason());
                }
            }
        }
        if (count == 0) {
            return false;
        }

        // We expect to have collected a task ID above
        if (!inUseTaskIdCollector.isPresent()) {
            invariants.inconsistent("In use IP allocation placement failure with empty in use task ID: failed taskId=%s", taskRequest.getId());
        }
        resultCollector.computeIfAbsent(FailureKind.WaitingForInUseIpAllocation, k -> new HashMap<>())
                .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                .add(new InUseIpAllocationConstraintFailure(taskRequest.getId(), inUseTaskIdCollector, count, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));

        return true;
    }

    private boolean processOpportunisticResources(T taskRequest, List<TaskAssignmentResult> assignmentResults,
                                                  Map<FailureKind, Map<T, List<TaskPlacementFailure>>> resultCollector) {
        int count = 0;
        for (TaskAssignmentResult assignmentResult : assignmentResults) {
            if (isOpportunisticResource(assignmentResult)) {
                count++;
            }
        }
        if (count == 0) {
            return false;
        }

        resultCollector.computeIfAbsent(FailureKind.OpportunisticResource, k -> new HashMap<>())
                .computeIfAbsent(taskRequest, k -> new ArrayList<>())
                .add(new TaskPlacementFailure(taskRequest.getId(), FailureKind.OpportunisticResource, count, SchedulerUtils.getTier((QueuableTask) taskRequest), buildRawDataMap(taskRequest, assignmentResults)));

        return true;
    }

    private Map<String, Object> buildRawDataMap(TaskRequest taskRequest, List<TaskAssignmentResult> assignmentResults) {
        Map<String, Object> rawData = new HashMap<>();
        rawData.put("taskRequest", taskRequest);
        rawData.put("assignmentResults", assignmentResults);
        return rawData;
    }

    private boolean canScheduleOnAgent(TaskAssignmentResult assignmentResult) {
        ConstraintFailure constraintFailure = assignmentResult.getConstraintFailure();
        if (constraintFailure == null || StringExt.isEmpty(constraintFailure.getReason())) {
            return true;
        }
        return !AgentManagementConstraint.isAgentManagementConstraintReason(constraintFailure.getReason());
    }

    private boolean isLaunchGuard(TaskAssignmentResult assignmentResult) {
        ConstraintFailure constraintFailure = assignmentResult.getConstraintFailure();
        if (constraintFailure == null || StringExt.isEmpty(constraintFailure.getReason())) {
            return false;
        }
        return AgentLaunchGuardConstraint.isAgentLaunchGuardConstraintReason(constraintFailure.getReason());
    }

    private boolean isOverAgentAvailableResources(AssignmentFailure assignmentFailure) {
        switch (assignmentFailure.getResource()) {
            case CPU:
            case Memory:
            case Network:
            case Ports:
            case Disk:
            case ResourceSet:
                return true;
        }
        return false;
    }

    private boolean isJobHardConstraint(TaskAssignmentResult assignmentResult, Set<String> constraintCollector) {
        ConstraintFailure constraintFailure = assignmentResult.getConstraintFailure();
        if (constraintFailure == null || StringExt.isEmpty(constraintFailure.getName())) {
            return false;
        }

        String name = constraintFailure.getName();
        if (name.equals(V3UniqueHostConstraint.NAME)
                || name.equals(V3ZoneBalancedHardConstraintEvaluator.NAME)
                || name.equals(EXCLUSIVE_HOST_CONSTRAINT_NAME)) {
            constraintCollector.add(name);
            return true;
        }

        return false;
    }

    private boolean isInUseIpAllocation(TaskAssignmentResult assignmentResult) {
        ConstraintFailure constraintFailure = assignmentResult.getConstraintFailure();
        if (constraintFailure == null || StringExt.isEmpty(constraintFailure.getReason())) {
            return false;
        }
        return IpAllocationConstraint.isInUseIpAllocationConstraintReason(constraintFailure.getReason());
    }

    private boolean isOpportunisticResource(TaskAssignmentResult assignmentResult) {
        ConstraintFailure constraintFailure = assignmentResult.getConstraintFailure();
        if (constraintFailure == null || StringExt.isEmpty(constraintFailure.getReason())) {
            return false;
        }
        return OpportunisticCpuConstraint.isOpportunisticCpuConstraintReason(constraintFailure.getReason());
    }

    private void writeToLog() {
        if (!loggingTokenBucket.tryTake()) {
            return;
        }

        Map<FailureKind, Map<T, List<TaskPlacementFailure>>> failures = failuresRef.get();
        if (failures.isEmpty()) {
            logger.info("Scheduling failure state dump: no failures");
            return;
        }

        logger.info("Scheduling failure state dump({}):", failures.values().stream().mapToInt(taskPlacementFailureMap -> taskPlacementFailureMap.values().size()).sum());
        for (Map.Entry<FailureKind, Map<T, List<TaskPlacementFailure>>> entry : failures.entrySet()) {
            FailureKind failureKind = entry.getKey();
            List<TaskPlacementFailure> kindFailures = entry.getValue().values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            logger.info("    {}({}):", failureKind, kindFailures.size());
            int loggedRecordCount = Math.min(kindFailures.size(), 20);
            kindFailures.subList(0, loggedRecordCount).forEach(failure -> {
                if (failure instanceof JobHardConstraintPlacementFailure) {
                    JobHardConstraintPlacementFailure constraintFailure = (JobHardConstraintPlacementFailure) failure;
                    logger.info(String.format(LOG_HARD_CONSTRAINT_FORMAT, failure.getTaskId(), failure.getAgentCount(), constraintFailure.getHardConstraints()));
                } else {
                    logger.info(String.format(LOG_FORMAT, failure.getTaskId(), failure.getAgentCount()));
                }
            });
            if (loggedRecordCount < kindFailures.size()) {
                logger.info("        skipping {} remaining items", kindFailures.size() - loggedRecordCount);
            }
        }
    }
}
