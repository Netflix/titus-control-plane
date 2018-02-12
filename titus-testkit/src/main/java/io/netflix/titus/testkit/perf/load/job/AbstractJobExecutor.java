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

package io.netflix.titus.testkit.perf.load.job;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.NotificationCase;
import com.netflix.titus.grpc.protogen.JobId;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.common.util.rx.RetryHandlerBuilder;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to kill task " + taskId + " of job " + name, e)))
                .doOnCompleted(() -> logger.info("Killed task {}", taskId));
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
