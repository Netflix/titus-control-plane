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
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.job.JobMgr;
import com.netflix.titus.master.job.V2JobOperations;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.mesos.TaskInfoFactory;
import com.netflix.titus.master.model.job.TitusQueuableTask;
import com.netflix.titus.master.store.InvalidJobStateChangeException;
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
    private final V2JobOperations v2JobOperations;
    private final V3JobOperations v3JobOperations;
    private final TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory;
    private final Clock clock;

    @Inject
    TaskPlacementRecorder(Config config,
                          MasterConfiguration masterConfiguration,
                          TaskSchedulingService schedulingService,
                          V2JobOperations v2JobOperations,
                          V3JobOperations v3JobOperations,
                          TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory,
                          TitusRuntime titusRuntime) {
        this.config = config;
        this.masterConfiguration = masterConfiguration;
        this.schedulingService = schedulingService;
        this.v2JobOperations = v2JobOperations;
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
            Map<AgentAssignment, List<Protos.TaskInfo>> v2Result = processV2Assignments(assignments);
            Map<AgentAssignment, List<Protos.TaskInfo>> v3Result = processV3Assignments(assignments);

            Set<AgentAssignment> allAssignments = CollectionsExt.merge(v2Result.keySet(), v3Result.keySet());

            return allAssignments.stream()
                    .map(a -> Pair.of(a.getLeases(), CollectionsExt.merge(v2Result.get(a), v3Result.get(a))))
                    .collect(Collectors.toList());
        } finally {
            int taskCount = schedulingResult.getResultMap().values().stream().mapToInt(a -> a.getTasksAssigned().size()).sum();
            logger.info("Task placement recording: tasks={}, executionTimeMs={}", taskCount, clock.wallTime() - startTime);
        }
    }

    private Map<AgentAssignment, List<Protos.TaskInfo>> processV2Assignments(List<AgentAssignment> assignments) {
        Map<AgentAssignment, List<Protos.TaskInfo>> result = new HashMap<>();
        for (AgentAssignment assignment : assignments) {
            List<Protos.TaskInfo> taskInfos = new ArrayList<>();
            for (TaskAssignmentResult assignmentResult : assignment.getV2Assignments()) {
                processV2Assignment(assignment, assignmentResult).ifPresent(taskInfos::add);
            }
            result.put(assignment, taskInfos);
        }
        return result;
    }

    private Optional<Protos.TaskInfo> processV2Assignment(AgentAssignment assignment, TaskAssignmentResult assignmentResult) {
        List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = assignmentResult.getrSets();
        TitusQueuableTask task = (TitusQueuableTask) assignmentResult.getRequest();

        boolean taskFound;
        PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult = consumeResults.get(0);
        final JobMgr jobMgr = v2JobOperations.getJobMgrFromTaskId(task.getId());
        taskFound = jobMgr != null;
        if (taskFound) {
            final VirtualMachineLease lease = assignment.getLeases().get(0);
            try {
                Protos.TaskInfo taskInfo = jobMgr.setLaunchedAndCreateTaskInfo(
                        task, lease.hostname(), assignment.getAttributesMap(), lease.getOffer().getSlaveId(),
                        consumeResult, assignmentResult.getAssignedPorts()
                );
                return Optional.of(taskInfo);
            } catch (InvalidJobStateChangeException | InvalidJobException e) {
                logger.warn("Not launching task due to error setting state to launched for " + task.getId() + " - " +
                        e.getMessage());
            } catch (Exception e) {
                // unexpected error creating task info
                String msg = "fatal error creating taskInfo for " + task.getId() + ": " + e.getMessage();
                logger.warn("Killing job " + jobMgr.getJobId() + ": " + msg, e);
                jobMgr.killJob("SYSTEM", msg);
            }
        } else {
            removeUnknownTask(assignmentResult, task);
        }

        return Optional.empty();
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
            removeUnknownTask(assignmentResult, fenzoTask);
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
                            executorUriOverrideOpt, attributesMap
                    ).apply(oldTask)
            ).toObservable().cast(Protos.TaskInfo.class).concatWith(Observable.fromCallable(() ->
                    v3TaskInfoFactory.newTaskInfo(
                            fenzoTask, v3Job, v3Task, lease.hostname(), attributesMap, lease.getOffer().getSlaveId(),
                            consumeResult, executorUriOverrideOpt)
            )).timeout(
                    STORE_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS
            ).onErrorResumeNext(error -> {
                Throwable recordTaskError = (Throwable) error; // Type inference issue
                if (JobManagerException.hasErrorCode(recordTaskError, JobManagerException.ErrorCode.UnexpectedTaskState)) {
                    // TODO More checking here. If it is actually running, we should kill it
                    logger.info("Not launching task, as it is no longer in Accepted state (probably killed): {}", v3Task.getId());
                } else {
                    if (error instanceof TimeoutException) {
                        logger.error("Timed out during writing task {} (job {}) status update to the store", fenzoTask.getId(), v3Job.getId());
                    } else {
                        logger.info("Not launching task due to model update failure: {}", v3Task.getId(), error);
                    }
                    killBrokenV3Task(fenzoTask, "model update error: " + recordTaskError.getMessage());
                }
                return Observable.empty();
            }).map(taskInfo -> Pair.of(assignment, taskInfo));
        } catch (Exception e) {
            killBrokenV3Task(fenzoTask, e.toString());
            logger.error("Fatal error when creating TaskInfo for task: {}", fenzoTask.getId(), e);
            return Observable.empty();
        }
    }

    private void killBrokenV3Task(TitusQueuableTask task, String reason) {
        v3JobOperations.killTask(task.getId(), false, String.format("Failed to launch task %s due to %s", task.getId(), reason)).subscribe(
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

    private void removeUnknownTask(TaskAssignmentResult assignmentResult, TitusQueuableTask task) {
        // job must have been terminated, remove task from Fenzo
        logger.warn("Rejecting assignment and removing task after not finding jobMgr for task: {}", task.getId());
        schedulingService.removeTask(task.getId(), task.getQAttributes(), assignmentResult.getHostname());
    }

    private class AgentAssignment {
        private final String hostname;
        private final Map<String, String> attributeMap;
        private final VMAssignmentResult assignmentResult;
        private final List<TaskAssignmentResult> v2Assignments;
        private final List<TaskAssignmentResult> v3Assignments;

        AgentAssignment(String hostname, VMAssignmentResult assignmentResult) {
            this.hostname = hostname;
            this.assignmentResult = assignmentResult;
            this.attributeMap = buildAttributeMap(assignmentResult);

            List<TaskAssignmentResult> v2Assignments = new ArrayList<>();
            List<TaskAssignmentResult> v3Assignments = new ArrayList<>();
            assignmentResult.getTasksAssigned().forEach(a -> {
                if (JobFunctions.isV2Task(a.getTaskId())) {
                    v2Assignments.add(a);
                } else {
                    v3Assignments.add(a);
                }
            });
            this.v2Assignments = v2Assignments;
            this.v3Assignments = v3Assignments;
        }

        List<VirtualMachineLease> getLeases() {
            return assignmentResult.getLeasesUsed();
        }

        Map<String, String> getAttributesMap() {
            return attributeMap;
        }

        List<TaskAssignmentResult> getV2Assignments() {
            return v2Assignments;
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
