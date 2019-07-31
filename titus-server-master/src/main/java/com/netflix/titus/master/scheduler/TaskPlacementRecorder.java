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

package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.archaius.api.Config;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskSchedulingService;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.mesos.TaskInfoFactory;
import com.netflix.titus.master.model.job.TitusQueuableTask;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Singleton
class TaskPlacementRecorder {

    private static final Logger logger = LoggerFactory.getLogger(TaskPlacementRecorder.class);

    private static final long STORE_UPDATE_TIMEOUT_MS = 5_000;
    private static final int RECORD_CONCURRENCY_LIMIT = 500;

    private final Config config;
    private final MasterConfiguration masterConfiguration;
    private final TaskSchedulingService schedulingService;
    private final V3JobOperations v3JobOperations;
    private final TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory;
    private final Clock clock;

    @Inject
    TaskPlacementRecorder(Config config,
                          MasterConfiguration masterConfiguration,
                          TaskSchedulingService schedulingService,
                          V3JobOperations v3JobOperations,
                          TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory,
                          TitusRuntime titusRuntime) {
        this.config = config;
        this.masterConfiguration = masterConfiguration;
        this.schedulingService = schedulingService;
        this.v3JobOperations = v3JobOperations;
        this.v3TaskInfoFactory = v3TaskInfoFactory;
        this.clock = titusRuntime.getClock();
    }

    List<Pair<List<VirtualMachineLease>, List<Protos.TaskInfo>>> record(SchedulingResult schedulingResult) {
        List<AgentAssignment> assignments = schedulingResult.getResultMap().entrySet().stream()
                .map(entry -> new AgentAssignment(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        long startTime = clock.wallTime();
        try {
            Map<AgentAssignment, List<Protos.TaskInfo>> v3Result = processV3Assignments(assignments);

            Set<AgentAssignment> allAssignments = v3Result.keySet();

            return allAssignments.stream()
                    .map(a -> Pair.of(a.getLeases(), v3Result.get(a)))
                    .collect(Collectors.toList());
        } finally {
            int taskCount = schedulingResult.getResultMap().values().stream().mapToInt(a -> a.getTasksAssigned().size()).sum();
            if (taskCount > 0) {
                logger.info("Task placement recording: tasks={}, executionTimeMs={}", taskCount, clock.wallTime() - startTime);
            }
        }
    }

    private Map<AgentAssignment, List<Protos.TaskInfo>> processV3Assignments(List<AgentAssignment> assignments) {
        List<Observable<Pair<AgentAssignment, Protos.TaskInfo>>> recordActions = assignments.stream()
                .flatMap(a -> a.getV3Assignments().stream().map(ar -> processTask(a, ar)))
                .collect(Collectors.toList());
        List<Pair<AgentAssignment, Protos.TaskInfo>> taskInfos = Observable.merge(recordActions, RECORD_CONCURRENCY_LIMIT).toList().toBlocking().first();

        Map<AgentAssignment, List<Protos.TaskInfo>> result = new HashMap<>();
        taskInfos.forEach(p -> result.computeIfAbsent(p.getLeft(), a -> new ArrayList<>()).add(p.getRight()));

        return result;
    }

    private Observable<Pair<AgentAssignment, Protos.TaskInfo>> processTask(
            AgentAssignment assignment,
            TaskAssignmentResult assignmentResult) {
        VirtualMachineLease lease = assignment.getLeases().get(0);
        TitusQueuableTask fenzoTask = (TitusQueuableTask) assignmentResult.getRequest();

        Optional<Pair<Job<?>, Task>> v3JobAndTask = v3JobOperations.findTaskById(fenzoTask.getId());
        if (!v3JobAndTask.isPresent()) {
            logger.warn("Rejecting assignment and removing task after not finding jobMgr for task: {}", fenzoTask.getId());
            removeTask(assignmentResult, fenzoTask);
            return Observable.empty();
        }

        Job<?> v3Job = v3JobAndTask.get().getLeft();
        Task v3Task = v3JobAndTask.get().getRight();

        try {
            PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult = assignmentResult.getrSets().get(0);

            Map<String, String> attributesMap = assignment.getAttributesMap();
            Optional<String> executorUriOverrideOpt = JobManagerUtil.getExecutorUriOverride(config, attributesMap);
            return v3JobOperations.recordTaskPlacement(
                    fenzoTask.getId(),
                    oldTask -> JobManagerUtil.newTaskLaunchConfigurationUpdater(
                            masterConfiguration.getHostZoneAttributeName(), lease, consumeResult,
                            executorUriOverrideOpt, attributesMap, getTierName(fenzoTask)
                    ).apply(oldTask),
                    JobManagerConstants.SCHEDULER_CALLMETADATA.toBuilder().withCallReason("Record task placement").build()
            ).toObservable().cast(Protos.TaskInfo.class).concatWith(Observable.fromCallable(() ->
                    v3TaskInfoFactory.newTaskInfo(
                            fenzoTask, v3Job, v3Task, lease.hostname(), attributesMap, lease.getOffer().getSlaveId(),
                            consumeResult, executorUriOverrideOpt)
            )).timeout(
                    STORE_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS
            ).onErrorResumeNext(error -> {
                Throwable recordTaskError = (Throwable) error; // Type inference issue
                if (JobManagerException.hasErrorCode(recordTaskError, JobManagerException.ErrorCode.UnexpectedTaskState)) {
                    logger.info("Not launching task: {} and removing from fenzo as it is no longer in Accepted state (probably killed)", v3Task.getId());
                    removeTask(assignmentResult, fenzoTask);
                } else {
                    if (error instanceof TimeoutException) {
                        logger.error("Timed out during writing task {} (job {}) status update to the store", fenzoTask.getId(), v3Job.getId());
                    } else {
                        logger.info("Not launching task due to model update failure: {}", v3Task.getId(), error);
                    }
                    killBrokenV3Task(fenzoTask, "model update error: " + ExceptionExt.toMessageChain(recordTaskError));
                }
                return Observable.empty();
            }).map(taskInfo -> Pair.of(assignment, taskInfo));
        } catch (Exception e) {
            killBrokenV3Task(fenzoTask, ExceptionExt.toMessageChain(e));
            logger.error("Fatal error when creating Mesos#TaskInfo for task: {}", fenzoTask.getId(), e);
            return Observable.empty();
        }
    }

    private void killBrokenV3Task(TitusQueuableTask task, String reason) {
        String fullReason = String.format("Killing broken task %s (%s)", task.getId(), reason);
        v3JobOperations.killTask(task.getId(), false, Trigger.Scheduler,
                JobManagerConstants.SCHEDULER_CALLMETADATA.toBuilder().withCallReason(fullReason).build()).subscribe(
                next -> {
                },
                e -> {
                    if (e instanceof JobManagerException) {
                        JobManagerException je = (JobManagerException) e;

                        // This means task is no longer around, so we can safely ignore this.
                        if (je.getErrorCode() == JobManagerException.ErrorCode.JobNotFound
                                || je.getErrorCode() == JobManagerException.ErrorCode.TaskNotFound
                                || je.getErrorCode() == JobManagerException.ErrorCode.TaskTerminating) {
                            return;
                        }
                    }
                    logger.warn("Attempted to terminate task in potentially inconsistent state due to failed launch process {} failed: {}", task.getId(), e.getMessage());
                },
                () -> logger.warn("Terminated task {} as launch operation could not be completed", task.getId())
        );
    }

    private void removeTask(TaskAssignmentResult assignmentResult, TitusQueuableTask task) {
        try {
            schedulingService.removeTask(task.getId(), task.getQAttributes(), assignmentResult.getHostname());
        } catch (Exception e) {
            logger.warn("Unexpected error when removing task from the Fenzo queue", e);
        }
    }

    private String getTierName(TitusQueuableTask task) {
        int tierNumber = task.getQAttributes().getTierNumber();
        switch (tierNumber) {
            case 0:
                return Tier.Critical.name();
            case 1:
                return Tier.Flex.name();
            default:
                return "Unknown";
        }
    }

    private class AgentAssignment {
        private final String hostname;
        private final Map<String, String> attributeMap;
        private final VMAssignmentResult assignmentResult;
        private final List<TaskAssignmentResult> v3Assignments;

        AgentAssignment(String hostname, VMAssignmentResult assignmentResult) {
            this.hostname = hostname;
            this.assignmentResult = assignmentResult;
            this.attributeMap = buildAttributeMap(assignmentResult);
            this.v3Assignments = new ArrayList<>(assignmentResult.getTasksAssigned());
        }

        List<VirtualMachineLease> getLeases() {
            return assignmentResult.getLeasesUsed();
        }

        Map<String, String> getAttributesMap() {
            return attributeMap;
        }

        List<TaskAssignmentResult> getV3Assignments() {
            return v3Assignments;
        }

        @Override
        public int hashCode() {
            return hostname.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof AgentAssignment)) {
                return false;
            }
            AgentAssignment a2 = (AgentAssignment) obj;
            return hostname.equals(a2.hostname);
        }


        private Map<String, String> buildAttributeMap(VMAssignmentResult assignmentResult) {
            final Map<String, Protos.Attribute> attributeMap = assignmentResult.getLeasesUsed().get(0).getAttributeMap();
            final Map<String, String> result = new HashMap<>();
            if (!attributeMap.isEmpty()) {
                for (Map.Entry<String, Protos.Attribute> entry : attributeMap.entrySet()) {
                    result.put(entry.getKey(), entry.getValue().getText().getValue());
                }
            }
            return result;
        }
    }
}
