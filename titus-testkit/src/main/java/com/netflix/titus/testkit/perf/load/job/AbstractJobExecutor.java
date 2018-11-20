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

package com.netflix.titus.testkit.perf.load.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.subjects.AsyncSubject;

public abstract class AbstractJobExecutor<E extends JobDescriptor.JobDescriptorExt> implements JobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobExecutor.class);

    protected final String jobId;
    protected final String name;
    protected final ExecutionContext context;

    private final Subscription observeSubscription;

    protected volatile boolean doRun = true;
    private volatile AsyncSubject<Void> jobCompleted = AsyncSubject.create();

    protected volatile Job<E> job;
    protected volatile List<Task> activeTasks = Collections.emptyList();

    protected AbstractJobExecutor(Job job,
                                  ExecutionContext context) {
        this.job = job;
        this.jobId = job.getId();
        this.context = context;
        this.name = buildJobUniqueName(job.getJobDescriptor());
        this.observeSubscription = observeJob();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public boolean isSubmitted() {
        return doRun && jobId != null;
    }

    @Override
    public List<Task> getActiveTasks() {
        return activeTasks;
    }

    @Override
    public Completable awaitJobCompletion() {
        return Completable.fromObservable(jobCompleted);
    }

    @Override
    public void shutdown() {
        if (doRun) {
            doRun = false;
            if (!observeSubscription.isUnsubscribed()) {
                this.activeTasks = Collections.emptyList();
                observeSubscription.unsubscribe();
                Throwable error = context.getJobManagementClient().killJob(jobId).toCompletable().get();
                if (error != null) {
                    logger.debug("Job {} cleanup failure", jobId, error);
                }
            }
        }
    }

    private Subscription observeJob() {
        return Observable.defer(() -> context.getJobManagementClient().observeJob(JobId.newBuilder().setId(jobId).build()))
                .doOnNext(event -> {
                    if (event.getNotificationCase() == NotificationCase.TASKUPDATE) {
                        com.netflix.titus.grpc.protogen.Task task = event.getTaskUpdate().getTask();
                        String originalId = task.getTaskContextMap().get(TaskAttributes.TASK_ATTRIBUTES_TASK_ORIGINAL_ID);

                        List<Task> newTaskList = new ArrayList<>();
                        activeTasks.forEach(t -> {
                            if (!t.getOriginalId().equals(originalId)) {
                                newTaskList.add(t);
                            }
                        });
                        newTaskList.add(V3GrpcModelConverters.toCoreTask(job, task));
                        this.activeTasks = newTaskList;
                    }
                })
                .filter(event -> event.getNotificationCase() == NotificationCase.JOBUPDATE)
                .materialize()
                .flatMap(notification -> {
                    switch (notification.getKind()) {
                        case OnNext:
                            return Observable.just(isJobCompletedEvent(notification.getValue()));
                        case OnError:
                            StatusRuntimeException ex = (StatusRuntimeException) notification.getThrowable();
                            return ex.getStatus().getCode() == Status.Code.NOT_FOUND
                                    ? Observable.just(true)
                                    : Observable.error(notification.getThrowable());
                    }
                    // case OnCompleted:
                    return Observable.error(new IllegalStateException("Unexpected end of stream for job " + jobId));
                })
                .takeUntil(completed -> completed)
                .ignoreElements()
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(1_000, 30_000, TimeUnit.MILLISECONDS)
                        .buildExponentialBackoff()
                )
                .doOnTerminate(() -> {
                    jobCompleted.onCompleted();
                    logger.info("Job {} completed", jobId);
                })
                .subscribe();
    }

    @Override
    public Observable<Void> killJob() {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient()
                .killJob(jobId)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to kill job " + name, e)))
                .doOnCompleted(() -> logger.info("Killed job {}", jobId));
    }

    @Override
    public Observable<Void> killTask(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient()
                .killTask(taskId, false)
                .onErrorResumeNext(e -> Observable.error(new IOException(String.format("Failed to kill task %s  of job %s: error=%s", taskId, name, e.getMessage()), e)))
                .doOnCompleted(() -> logger.info("Killed task {}", taskId));
    }

    @Override
    public Mono<Void> evictTask(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getEvictionServiceClient()
                .terminateTask(taskId, "Simulator")
                .onErrorResume(e -> Mono.error(new IOException(String.format("Failed to evict task %s  of job %s: error=%s", taskId, name, e.getMessage()), e)))
                .doOnSuccess(nothing -> logger.info("Killed task {}", taskId));
    }

    private boolean isJobCompletedEvent(JobChangeNotification event) {
        if (event.getNotificationCase() != NotificationCase.JOBUPDATE) {
            return false;
        }
        return event.getJobUpdate().getJob().getStatus().getState() == com.netflix.titus.grpc.protogen.JobStatus.JobState.Finished;
    }

    private static String buildJobUniqueName(JobDescriptor<?> jobSpec) {
        return jobSpec.getApplicationName() + '-'
                + jobSpec.getJobGroupInfo().getStack() + '-'
                + jobSpec.getJobGroupInfo().getDetail() + '-'
                + jobSpec.getJobGroupInfo().getSequence();
    }
}
