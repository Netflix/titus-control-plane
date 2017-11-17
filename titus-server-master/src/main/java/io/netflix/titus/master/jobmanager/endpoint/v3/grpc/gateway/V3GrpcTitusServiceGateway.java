/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.ConstraintViolation;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.jobmanager.model.event.JobClosedEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction.Model;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.endpoint.common.TaskSummary;
import io.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.runtime.endpoint.v3.grpc.query.V3JobQueryCriteriaEvaluator;
import io.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static io.netflix.titus.runtime.TitusEntitySanitizerModule.JOB_SANITIZER;
import static java.util.Collections.singletonList;

/**
 */
@Singleton
public class V3GrpcTitusServiceGateway implements GrpcTitusServiceGateway {

    private static final Logger logger = LoggerFactory.getLogger(V3GrpcTitusServiceGateway.class);

    public static JobChangeNotification SNAPSHOT_END_MARKER = JobChangeNotification.newBuilder().setSnapshotEnd(
            JobChangeNotification.SnapshotEnd.newBuilder()
    ).build();

    private final V3JobOperations jobOperations;
    private final JobSubmitLimiter jobSubmitLimiter;
    private final LogStorageInfo<Task> logStorageInfo;
    private final EntitySanitizer entitySanitizer;

    @Inject
    public V3GrpcTitusServiceGateway(V3JobOperations jobOperations,
                                     JobSubmitLimiter jobSubmitLimiter,
                                     LogStorageInfo<Task> logStorageInfo,
                                     @Named(JOB_SANITIZER) EntitySanitizer entitySanitizer) {
        this.jobOperations = jobOperations;
        this.jobSubmitLimiter = jobSubmitLimiter;
        this.logStorageInfo = logStorageInfo;
        this.entitySanitizer = entitySanitizer;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        io.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
        }
        io.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);

        Set<ConstraintViolation<io.netflix.titus.api.jobmanager.model.job.JobDescriptor>> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }
        Optional<String> limited = jobSubmitLimiter.checkIfAllowed(sanitizedCoreJobDescriptor);
        if (limited.isPresent()) {
            return Observable.error(JobManagerException.jobCreateLimited(limited.get()));
        }
        return jobOperations.createJob(sanitizedCoreJobDescriptor);
    }

    @Override
    public Observable<Void> killJob(String user, String jobId) {
        return jobOperations.killJob(jobId);
    }

    @Override
    public Observable<Job> findJobById(String jobId, boolean includeArchivedTasks, Set<TaskStatus.TaskState> taskStates) {
        return jobOperations.getJob(jobId)
                .map(j -> Observable.just(V3GrpcModelConverters.toGrpcJob(j)))
                .orElseGet(() -> Observable.error(JobManagerException.jobNotFound(jobId)));
    }

    @Override
    public Pair<List<Job>, Pagination> findJobsByCriteria(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Optional<Page> pageOpt) {
        Page page = pageOpt.get();
        int offset = page.getPageSize() * page.getPageNumber();

        List<io.netflix.titus.api.jobmanager.model.job.Job<?>> queryResult = jobOperations.findJobs(
                new V3JobQueryCriteriaEvaluator(queryCriteria),
                offset,
                page.getPageSize() + 1
        );

        // We took extra item to know if there are more data to return
        boolean hasMore = queryResult.size() > page.getPageSize();
        List<io.netflix.titus.api.jobmanager.model.job.Job<?>> pageResult = hasMore ? queryResult.subList(0, page.getPageSize()) : queryResult;

        List<Job> jobs = pageResult.stream().map(V3GrpcModelConverters::toGrpcJob).collect(Collectors.toList());
        //TODO the pagination model here is not semantically correct since the total is not even known
        return Pair.of(jobs, new Pagination(page, false, 1, jobs.size()));
    }

    @Override
    public Pair<List<com.netflix.titus.grpc.protogen.Task>, Pagination> findTasksByCriteria(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Optional<Page> pageOpt) {
        Page page = pageOpt.get();
        int offset = page.getPageSize() * page.getPageNumber();

        List<Pair<io.netflix.titus.api.jobmanager.model.job.Job<?>, Task>> queryResult = jobOperations.findTasks(
                new V3TaskQueryCriteriaEvaluator(queryCriteria),
                offset,
                page.getPageSize() + 1
        );

        // We took an extra item to know if there is more data to return
        boolean hasMore = queryResult.size() > page.getPageSize();
        List<Pair<io.netflix.titus.api.jobmanager.model.job.Job<?>, Task>> pageResult = hasMore ? queryResult.subList(0, page.getPageSize()) : queryResult;

        List<com.netflix.titus.grpc.protogen.Task> tasks = pageResult.stream().map(jobTaskPair ->
                V3GrpcModelConverters.toGrpcTask(jobTaskPair.getRight(), logStorageInfo)
        ).collect(Collectors.toList());
        //TODO the pagination model here is not semantically correct since the total is not even known
        return Pair.of(tasks, new Pagination(page, false, 1, tasks.size()));
    }

    @Override
    public Observable<com.netflix.titus.grpc.protogen.Task> findTaskById(String taskId) {
        return jobOperations.findTaskById(taskId)
                .map(p -> {
                    Task task = p.getRight();
                    return Observable.just(V3GrpcModelConverters.toGrpcTask(task, logStorageInfo));
                })
                .orElseGet(() -> Observable.error(JobManagerException.taskNotFound(taskId)));
    }

    @Override
    public Observable<Void> resizeJob(String user, String jobId, int desired, int min, int max) {
        return jobOperations.updateJobCapacity(
                jobId,
                JobModel.newCapacity().withMin(min).withDesired(desired).withMax(max).build()
        );
    }

    @Override
    public Observable<Void> changeJobInServiceStatus(String user, String serviceJobId, boolean inService) {
        return jobOperations.updateJobStatus(serviceJobId, inService);
    }

    @Override
    public Observable<Void> killTask(String user, String taskId, boolean shrink) {
        return jobOperations.killTask(taskId, shrink, String.format("User %s initiated task kill", user));
    }

    @Override
    public Observable<List<TaskSummary>> getTaskSummary() {
        return Observable.error(new IllegalStateException("Operation not supported by V3 API"));
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return jobOperations.observeJobs()
                .filter(this::isChangeNotificationEvent)
                .flatMap(event -> {
                    if (event instanceof JobUpdateEvent) {
                        return Observable.from(toGrpcJobEvent((JobUpdateEvent) event));
                    }
                    return Observable.from(toGrpcTaskEvent((TaskUpdateEvent) event));
                })
                .compose(ObservableExt.head(() -> {
                    List<JobChangeNotification> snapshot = createJobsSnapshot();
                    snapshot.add(SNAPSHOT_END_MARKER);
                    return snapshot;
                }))
                .doOnError(e -> logger.error("Unexpected error in jobs event stream", e));
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return jobOperations.observeJob(jobId)
                .takeUntil(event -> event instanceof JobClosedEvent)
                .filter(this::isChangeNotificationEvent)
                .flatMap(event -> {
                    if (event instanceof JobUpdateEvent) {
                        return Observable.from(toGrpcJobEvent((JobUpdateEvent) event));
                    }
                    return Observable.from(toGrpcTaskEvent((TaskUpdateEvent) event));
                })
                .compose(ObservableExt.head(() -> {
                    List<JobChangeNotification> snapshot = createJobSnapshot(jobId);
                    snapshot.add(SNAPSHOT_END_MARKER);
                    return snapshot;
                }))
                .doOnError(e -> logger.error("Unexpected error in job {} event stream", jobId, e));
    }

    private boolean isChangeNotificationEvent(JobManagerEvent event) {
        ReconcilerEvent.EventType eventType = event.getEventType();
        if (eventType != ReconcilerEvent.EventType.ModelUpdated
                && eventType != ReconcilerEvent.EventType.ModelInitial) {
            return false;
        }
        if (event instanceof JobUpdateEvent) {
            return ((JobUpdateEvent) event).getModel() == Model.Reference;
        }
        if (event instanceof TaskUpdateEvent) {
            return ((TaskUpdateEvent) event).getModel() == Model.Reference;
        }
        return false;
    }

    private List<JobChangeNotification> createJobsSnapshot() {
        List<JobChangeNotification> snapshot = new ArrayList<>();

        List<io.netflix.titus.api.jobmanager.model.job.Job> coreJobs = jobOperations.getJobs();
        coreJobs.forEach(coreJob -> snapshot.add(toJobChangeNotification(coreJob)));

        List<Task> coreTasks = jobOperations.getTasks();
        coreTasks.forEach(task -> snapshot.add(toJobChangeNotification(task)));

        return snapshot;
    }

    private List<JobChangeNotification> createJobSnapshot(String jobId) {
        List<JobChangeNotification> snapshot = new ArrayList<>();

        io.netflix.titus.api.jobmanager.model.job.Job<?> coreJob = jobOperations.getJob(jobId).orElseThrow(() -> new IllegalArgumentException("Job with id " + jobId + " not found"));
        snapshot.add(toJobChangeNotification(coreJob));

        List<Task> coreTasks = jobOperations.getTasks(jobId);
        coreTasks.forEach(task -> snapshot.add(toJobChangeNotification(task)));

        return snapshot;
    }

    private List<JobChangeNotification> toGrpcJobEvent(JobUpdateEvent event) {
        return event.getJob()
                .map(coreJob -> {
                    boolean identical = event.getPreviousJobVersion().map(previous -> previous == coreJob).orElse(false);
                    return identical ? Collections.<JobChangeNotification>emptyList() : singletonList(toJobChangeNotification(coreJob));
                })
                .orElse(Collections.emptyList());
    }

    private List<JobChangeNotification> toGrpcTaskEvent(TaskUpdateEvent event) {
        return event.getTask()
                .map(coreTask -> {
                    boolean identical = event.getPreviousTaskVersion().map(previous -> previous == coreTask).orElse(false);
                    return identical ? Collections.<JobChangeNotification>emptyList() : singletonList(toJobChangeNotification(coreTask));
                })
                .orElse(Collections.emptyList());
    }

    private JobChangeNotification toJobChangeNotification(io.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        Job grpcJob = V3GrpcModelConverters.toGrpcJob(coreJob);
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(grpcJob))
                .build();
    }

    private JobChangeNotification toJobChangeNotification(Task coreTask) {
        com.netflix.titus.grpc.protogen.Task grpcTask = V3GrpcModelConverters.toGrpcTask(coreTask, logStorageInfo);
        return JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(grpcTask))
                .build();
    }
}
