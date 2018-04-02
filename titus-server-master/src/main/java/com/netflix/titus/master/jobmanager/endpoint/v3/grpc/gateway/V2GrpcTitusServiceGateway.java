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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.ConstraintViolation;

import com.google.common.base.Preconditions;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.model.event.JobStateChangeEvent;
import com.netflix.titus.api.model.event.SchedulingEvent;
import com.netflix.titus.api.model.event.TaskStateChangeEvent;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.WorkerNaming;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.api.service.TitusServiceException.ErrorCode;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.ApiOperations;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.master.endpoint.common.TaskSummary;
import com.netflix.titus.master.endpoint.v2.V2EngineTitusServiceGateway;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.job.V2JobOperations;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.query.V2JobQueryCriteriaEvaluator;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.query.V2TaskQueryCriteriaEvaluator;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.common.util.CollectionsExt.first;
import static com.netflix.titus.master.endpoint.common.TitusServiceGatewayUtil.newObservable;

/**
 * {@link TitusServiceGateway} implementation bridging GRPC/v3 API to legacy Titus runtime.
 */
@SuppressWarnings("unchecked")
public class V2GrpcTitusServiceGateway
        extends V2EngineTitusServiceGateway<String, JobDescriptor, JobDescriptor.JobSpecCase, Job, Task, TaskStatus.TaskState>
        implements GrpcTitusServiceGateway {

    private static final Logger logger = LoggerFactory.getLogger(V2GrpcTitusServiceGateway.class);

    /**
     * Service jobs have single stage with id '1'.
     */
    private static final int SERVICE_STAGE = 1;

    private final MasterConfiguration configuration;
    private final V2JobOperations v2JobOperations;
    private final JobSubmitLimiter jobSubmitLimiter;
    private final ApiOperations apiOperations;
    private final RxEventBus eventBus;
    private final LogStorageInfo<V2WorkerMetadata> logStorageInfo;
    private final EntitySanitizer entitySanitizer;

    @Inject
    public V2GrpcTitusServiceGateway(MasterConfiguration configuration,
                                     V2JobOperations v2JobOperations,
                                     JobSubmitLimiter jobSubmitLimiter,
                                     ApiOperations apiOperations,
                                     RxEventBus eventBus,
                                     LogStorageInfo<V2WorkerMetadata> logStorageInfo,
                                     @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer) {
        super(apiOperations);
        this.configuration = configuration;
        this.v2JobOperations = v2JobOperations;
        this.jobSubmitLimiter = jobSubmitLimiter;
        this.apiOperations = apiOperations;
        this.eventBus = eventBus;
        this.logStorageInfo = logStorageInfo;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        return newObservable(subscriber -> {
            V2JobDefinition jobDefinition = null;

            Optional<String> reserveStatus = null;
            try {
                // Map to the new core model to validate the data.
                com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
                com.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitized = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);
                Set<ConstraintViolation<com.netflix.titus.api.jobmanager.model.job.JobDescriptor>> violations = entitySanitizer.validate(sanitized);
                if (!violations.isEmpty()) {
                    subscriber.onError(TitusServiceException.invalidArgument(violations));
                    return;
                }

                // We map back to GRPC/protobuf model to propagated sanitizer updates to V2 engine.
                jobDefinition = V2GrpcModelConverters.toV2JobDefinition(V3GrpcModelConverters.toGrpcJobDescriptor(sanitized));

                reserveStatus = jobSubmitLimiter.reserveId(jobDefinition);
                if (reserveStatus.isPresent()) {
                    subscriber.onError(TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, reserveStatus.get()).build());
                    return;
                }
                Optional<String> limited = jobSubmitLimiter.checkIfAllowed(jobDefinition);

                if (limited.isPresent()) {
                    subscriber.onError(TitusServiceException.newBuilder(ErrorCode.INVALID_ARGUMENT, limited.get()).build());
                    return;
                }

                String jobId = v2JobOperations.submit(jobDefinition);
                subscriber.onNext(jobId);
                subscriber.onCompleted();
            } catch (IllegalArgumentException e) {
                subscriber.onError(TitusServiceException.invalidArgument(e));
            } finally {
                if (reserveStatus != null && !reserveStatus.isPresent()) {
                    jobSubmitLimiter.releaseId(jobDefinition);
                }
            }
        });
    }

    @Override
    public Observable<Void> killJob(String user, String jobId) {
        return newObservable(subscriber -> {
            if (apiOperations.killJob(jobId, user)) {
                subscriber.onCompleted();
            } else {
                subscriber.onError(TitusServiceException.jobNotFound(jobId));
            }
        });
    }

    @Override
    public Observable<Job> findJobById(String jobId, boolean includeArchivedTasks, Set<TaskStatus.TaskState> taskStates) {
        return newObservable(subscriber -> {
            V2JobMetadata jobMetadata = apiOperations.getJobMetadata(jobId);
            if (jobMetadata == null) {
                subscriber.onError(TitusServiceException.jobNotFound(jobId));
                return;
            }
            Job job = V2GrpcModelConverters.toGrpcJob(jobMetadata);

            subscriber.onNext(job);
            subscriber.onCompleted();
        });
    }

    @Override
    public Pair<List<Job>, Pagination> findJobsByCriteria(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Optional<Page> optionalPage) {
        Page page = checkPageIsPresent(optionalPage);

        Optional<Pair<List<Job>, Pagination>> emptyReply = sendEmptyReplyIfAlwaysEmpty(queryCriteria, page);
        if (emptyReply.isPresent()) {
            return emptyReply.get();
        }

        Set<String> jobIds = queryCriteria.getJobIds();
        boolean hasJobIds = !jobIds.isEmpty();
        List<V2JobMetadata> jobs = hasJobIds ? getSortedJobsByIds(jobIds) : getAllSortedJobs();

        V2JobQueryCriteriaEvaluator criteriaEvaluator = new V2JobQueryCriteriaEvaluator(queryCriteria);
        List<Pair<V2JobMetadata, List<V2WorkerMetadata>>> filtered = jobs.stream()
                .map(job -> {
                    List<V2WorkerMetadata> tasks = new ArrayList<>(job.getStageMetadata(1).getAllWorkers());
                    return Pair.of(job, tasks);
                })
                .filter(criteriaEvaluator)
                .collect(Collectors.toList());

        // Do not generate cursor for V2 model, as this is done in the routing layer when V2 and V3 data are merged.
        Pair<List<Pair<V2JobMetadata, List<V2WorkerMetadata>>>, Pagination> paginationPair = PaginationUtil.takePageWithoutCursor(page, filtered, p -> "");

        List<Job> grpcJobs = paginationPair.getLeft().stream()
                .map(jobAndTasks -> V2GrpcModelConverters.toGrpcJob(jobAndTasks.getLeft()))
                .collect(Collectors.toList());

        return Pair.of(grpcJobs, paginationPair.getRight());
    }

    @Override
    public Pair<List<Task>, Pagination> findTasksByCriteria(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Optional<Page> optionalPage) {
        Page page = checkPageIsPresent(optionalPage);

        Optional<Pair<List<Task>, Pagination>> emptyReply = sendEmptyReplyIfAlwaysEmpty(queryCriteria, page);
        if (emptyReply.isPresent()) {
            return emptyReply.get();
        }

        Set<String> jobIds = queryCriteria.getJobIds();
        boolean hasJobIds = !jobIds.isEmpty();
        List<V2JobMetadata> jobs = hasJobIds ? getSortedJobsByIds(jobIds) : getAllSortedJobs();
        boolean includeArchived = queryCriteria.getTaskStates().contains(TaskStatus.TaskState.Finished);

        V2TaskQueryCriteriaEvaluator criteriaEvaluator = new V2TaskQueryCriteriaEvaluator(queryCriteria);
        List<Pair<V2JobMetadata, V2WorkerMetadata>> filtered = jobs.stream()
                .flatMap(job -> getSortedWorkersForJob(job, includeArchived).stream()
                        .map(task -> Pair.of(job, task))
                )
                .filter(criteriaEvaluator)
                .collect(Collectors.toList());

        Pair<List<Pair<V2JobMetadata, V2WorkerMetadata>>, Pagination> paginationPair = PaginationUtil.takePageWithoutCursor(page, filtered, task -> "");

        List<Task> grpcTasks = paginationPair.getLeft().stream()
                .map(jobAndTasks -> V2GrpcModelConverters.toGrpcTask(configuration, jobAndTasks.getRight(), logStorageInfo))
                .collect(Collectors.toList());

        return Pair.of(grpcTasks, paginationPair.getRight());
    }

    private <ITEM> Optional<Pair<List<ITEM>, Pagination>> sendEmptyReplyIfAlwaysEmpty(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria,
                                                                                      Page page) {
        // If client asks only for jobs in 'KillInitiated' state, return empty reply as V2 does not support this state.
        if (queryCriteria.getJobState().isPresent()) {
            JobStatus.JobState jobState = ((JobStatus.JobState) queryCriteria.getJobState().get());
            if (jobState == JobStatus.JobState.KillInitiated) {
                return Optional.of(Pair.of(Collections.emptyList(), Pagination.empty(page)));
            }
        }
        // If client asks only for tasks in 'KillInitiated' state, return empty reply as V2 does not support this state.
        if (queryCriteria.getTaskStates().size() == 1 && first(queryCriteria.getTaskStates()) == TaskStatus.TaskState.KillInitiated) {
            return Optional.of(Pair.of(Collections.emptyList(), Pagination.empty(page)));
        }
        return Optional.empty();
    }

    private List<V2JobMetadata> getAllSortedJobs() {
        List<V2JobMetadata> allJobs = apiOperations.getAllJobsMetadata(true, Integer.MAX_VALUE);
        allJobs.sort(Comparator.comparing(V2JobMetadata::getJobId));
        return allJobs;
    }

    private List<V2JobMetadata> getSortedJobsByIds(Set<String> jobIds) {
        List<V2JobMetadata> jobs = new ArrayList<>();
        for (String jobId : jobIds) {
            // only try to find v2 jobs
            if (JobFunctions.isV2JobId(jobId)) {
                V2JobMetadata jobMetadata = apiOperations.getJobMetadata(jobId);
                if (jobMetadata != null) {
                    jobs.add(jobMetadata);
                }
            }
        }
        jobs.sort(Comparator.comparing(V2JobMetadata::getJobId));
        return jobs;
    }

    private List<V2WorkerMetadata> getSortedWorkersForJob(V2JobMetadata job, boolean includeArchived) {
        List<V2WorkerMetadata> workers = new ArrayList<>(job.getStageMetadata(1).getAllWorkers());
        if (includeArchived) {
            final Set<String> currentWorkerInstanceIds = workers.stream().map(V2WorkerMetadata::getWorkerInstanceId).collect(Collectors.toCollection(HashSet::new));
            final List<? extends V2WorkerMetadata> archivedWorkers = apiOperations.getArchivedWorkers(job.getJobId());
            final List<? extends V2WorkerMetadata> archivedWorkersOnly = archivedWorkers.stream().filter(w -> !currentWorkerInstanceIds.contains(w.getWorkerInstanceId())).collect(Collectors.toList());
            workers.addAll(archivedWorkersOnly);
        }
        workers.sort(Comparator.comparing(V2WorkerMetadata::getWorkerInstanceId));
        return workers;
    }

    private Page checkPageIsPresent(Optional<Page> optionalPage) {
        Preconditions.checkArgument(optionalPage.isPresent(), "Expected page object");
        Page page = optionalPage.get();
        Preconditions.checkArgument(page.getPageNumber() >= 0, "Page number cannot be negative");
        Preconditions.checkArgument(page.getPageSize() > 0, "Page size must be > 0");
        return page;
    }

    @Override
    public Observable<Task> findTaskById(String taskId) {
        return newObservable(subscriber -> {
            V2WorkerMetadata v2Task;
            try {
                v2Task = findWorker(taskId).getRight();
            } catch (TitusServiceException e) {
                subscriber.onError(e);
                return;
            }

            Task task = V2GrpcModelConverters.toGrpcTask(configuration, v2Task, logStorageInfo);
            subscriber.onNext(task);
            subscriber.onCompleted();
        });
    }

    @Override
    public Observable<Void> resizeJob(String user, String jobId, int desired, int min, int max) {
        return newObservable(subscriber -> {
            try {
                apiOperations.updateInstanceCounts(jobId, SERVICE_STAGE, min, desired, max, user);
                subscriber.onCompleted();
            } catch (InvalidJobException e) {
                subscriber.onError(TitusServiceException.jobNotFound(jobId, e));
            }
        });
    }

    @Override
    public Observable<Void> updateJobProcesses(String user, String jobId, boolean disableDecreaseDesired, boolean disableIncreaseDesired) {
        return newObservable(subscriber -> {
            try {
                apiOperations.updateJobProcesses(jobId, SERVICE_STAGE, disableIncreaseDesired, disableDecreaseDesired, user);
                subscriber.onCompleted();
            } catch (Exception e) {
                subscriber.onError(TitusServiceException.jobNotFound(jobId, e));
            }
        });
    }

    @Override
    public Observable<Void> changeJobInServiceStatus(String user, String serviceJobId, boolean inService) {
        return newObservable(subscriber -> {
            try {
                apiOperations.updateInServiceStatus(serviceJobId, 1, inService, user);
                subscriber.onCompleted();
            } catch (InvalidJobException e) {
                subscriber.onError(TitusServiceException.jobNotFound(serviceJobId, e));
            }
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return emitJobStateUpdates().compose(ObservableExt.head(this::emitAllJobSnapshots));
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return Observable.fromCallable(() -> {
            V2JobMgrIntf jobManager = v2JobOperations.getJobMgr(jobId);
            V2JobMetadata jobMetadata;
            if (jobManager == null || (jobMetadata = jobManager.getJobMetadata()) == null) {
                throw TitusServiceException.jobNotFound(jobId);
            }
            List<V2WorkerMetadata> tasks = (List<V2WorkerMetadata>) jobManager.getWorkers();
            return Pair.of(jobMetadata, tasks);
        }).flatMap(pair -> {
                    V2JobMetadata jobMetadata = pair.getLeft();
                    List<V2WorkerMetadata> tasks = pair.getRight();
                    return Observable.fromCallable(() -> emitJobSnapshotEvent(jobMetadata, tasks))
                            .flatMap(Observable::from)
                            .concatWith(emitJobStateUpdates(jobMetadata));
                }
        );
    }

    private List<JobChangeNotification> emitAllJobSnapshots() {
        List<JobChangeNotification> jobUpdateEvents = v2JobOperations.getAllJobMgrs().stream()
                .map(V2JobMgrIntf::getJobMetadata)
                .filter(Objects::nonNull)
                .map(V2GrpcModelConverters::toGrpcJobNotification)
                .collect(Collectors.toList());

        // Collect tasks separately, as we want to emit first all jobs, and only after tasks.
        List<JobChangeNotification> taskUpdateEvents = v2JobOperations.getAllJobMgrs().stream()
                .map(V2JobMgrIntf::getWorkers)
                .flatMap(workers -> workers.stream().map(this::createTaskUpdateEvent))
                .collect(Collectors.toList());

        return CollectionsExt.merge(jobUpdateEvents, taskUpdateEvents, Collections.singletonList(V3GrpcTitusServiceGateway.SNAPSHOT_END_MARKER));
    }

    private List<JobChangeNotification> emitJobSnapshotEvent(V2JobMetadata jobMetadata, List<V2WorkerMetadata> tasks) {
        List<JobChangeNotification> events = new ArrayList<>();

        events.add(V2GrpcModelConverters.toGrpcJobNotification(jobMetadata));
        tasks.forEach(task -> events.add(createTaskUpdateEvent(task)));
        events.add(V3GrpcTitusServiceGateway.SNAPSHOT_END_MARKER);

        return events;
    }

    private JobChangeNotification createTaskUpdateEvent(V2WorkerMetadata task) {
        return JobChangeNotification.newBuilder().setTaskUpdate(
                JobChangeNotification.TaskUpdate.newBuilder().setTask(V2GrpcModelConverters.toGrpcTask(configuration, task, logStorageInfo))
        ).build();
    }

    private Observable<JobChangeNotification> emitJobStateUpdates() {
        return eventBus.listen("v2GrpcGateway", SchedulingEvent.class).flatMap(this::handleEvent);
    }

    private Observable<JobChangeNotification> emitJobStateUpdates(V2JobMetadata jobMetadata) {
        return eventBus.listen("v2GrpcGateway", SchedulingEvent.class)
                .filter(e -> isEventOf(jobMetadata.getJobId(), e))
                .flatMap(this::handleEvent)
                .takeUntil(this::isTerminateEvent);
    }

    private Observable<JobChangeNotification> handleEvent(SchedulingEvent event) {
        if (isJobEvent(event)) {
            return handleJobEvent((JobStateChangeEvent) event).map(Observable::just).orElse(Observable.empty());
        } else if (isTaskEvent(event)) {
            return handleTaskEvent((TaskStateChangeEvent<?, ?>) event).map(Observable::just).orElse(Observable.empty());
        }
        return Observable.empty();
    }

    private Optional<JobChangeNotification> handleJobEvent(JobStateChangeEvent<?> event) {
        String jobId = event.getJobId();
        V2JobMetadata jobMetadata = (V2JobMetadata) event.getSource();
        if (jobMetadata == null) {
            V2JobMgrIntf jobMgr = v2JobOperations.getJobMgr(jobId);
            if (jobMgr == null || (jobMetadata = jobMgr.getJobMetadata()) == null) {
                logger.warn("Job manager not found for event {}", event);
                return Optional.empty();
            }
        }
        return Optional.of(V2GrpcModelConverters.toGrpcJobNotification(jobMetadata));
    }

    private Optional<JobChangeNotification> handleTaskEvent(TaskStateChangeEvent<?, ?> event) {
        WorkerNaming.JobWorkerIdPair jobAndWorkerId = WorkerNaming.getJobAndWorkerId(event.getTaskId());
        if (jobAndWorkerId.workerIndex == -1) {
            logger.warn("Bad worker id for event {}", event);
            return Optional.empty();
        }
        String jobId = jobAndWorkerId.jobId;
        Pair<V2JobMetadata, V2WorkerMetadata> jobAndTaskPair = (Pair<V2JobMetadata, V2WorkerMetadata>) event.getSource();
        V2JobMetadata job = jobAndTaskPair.getLeft();
        V2WorkerMetadata task = jobAndTaskPair.getRight();
        if (task == null) {
            V2JobMgrIntf jobMgr = v2JobOperations.getJobMgr(jobId);
            if (jobMgr == null || job == null) {
                logger.warn("Job manager not found for event {}", event);
                return Optional.empty();
            }
            task = job.getStageMetadata(1).getAllWorkers().stream()
                    .filter(t -> t.getWorkerNumber() == jobAndWorkerId.workerNumber && t.getWorkerIndex() == jobAndWorkerId.workerIndex)
                    .findFirst().orElse(null);
            if (task == null) {
                logger.warn("Task not found for event {}", event);
                return Optional.empty();
            }
        }

        return Optional.of(createTaskUpdateEvent(task));
    }

    private boolean isJobEvent(SchedulingEvent event) {
        return event instanceof JobStateChangeEvent && event.getSource() instanceof V2JobMetadata;
    }

    private boolean isTaskEvent(SchedulingEvent<?> event) {
        return event instanceof TaskStateChangeEvent<?, ?> && event.getSource() instanceof Pair;
    }

    private boolean isEventOf(String jobId, SchedulingEvent<?> event) {
        return event.getJobId().equals(jobId);
    }

    private boolean isTerminateEvent(JobChangeNotification event) {
        if (event.getNotificationCase() != JobChangeNotification.NotificationCase.JOBUPDATE) {
            return false;
        }
        JobStatus.JobState jobState = event.getJobUpdate().getJob().getStatus().getState();
        return jobState == JobStatus.JobState.Finished;
    }

    @Override
    public Observable<List<TaskSummary>> getTaskSummary() {
        throw TitusServiceException.notSupported();
    }
}
