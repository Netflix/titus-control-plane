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

package com.netflix.titus.testkit.perf.load.runner.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.testkit.perf.load.runner.LoadGeneratorConstants.JOB_TERMINATOR_CALL_METADATA;
import static com.netflix.titus.testkit.perf.load.runner.LoadGeneratorConstants.TEST_CALL_METADATA;

public abstract class AbstractJobExecutor<E extends JobDescriptor.JobDescriptorExt> implements JobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobExecutor.class);

    protected final String jobId;
    protected final String name;
    protected final ExecutionContext context;

    private final Disposable observeSubscription;
    private final MonoProcessor<Void> jobCompleted = MonoProcessor.create();

    protected volatile boolean doRun = true;

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
    public Mono<Void> awaitJobCompletion() {
        return jobCompleted;
    }

    @Override
    public void shutdown() {
        if (doRun) {
            doRun = false;
            if (!observeSubscription.isDisposed()) {
                this.activeTasks = Collections.emptyList();
                observeSubscription.dispose();
                try {
                    context.getJobManagementClient().killJob(jobId, JOB_TERMINATOR_CALL_METADATA).block();
                } catch (RuntimeException e) {
                    logger.debug("Job {} cleanup failure", jobId, e);
                }
            }
        }
    }

    private Disposable observeJob() {
        return Flux.defer(() -> context.getJobManagementClient().observeJob(jobId))
                .doOnNext(event -> {
                    if (event instanceof TaskUpdateEvent) {
                        TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                        Task task = taskUpdateEvent.getCurrent();
                        String originalId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_TASK_ORIGINAL_ID);

                        List<Task> newTaskList = new ArrayList<>();
                        activeTasks.forEach(t -> {
                            if (!t.getOriginalId().equals(originalId)) {
                                newTaskList.add(t);
                            }
                        });
                        newTaskList.add(task);
                        this.activeTasks = newTaskList;
                    }
                })
                .filter(event -> event instanceof JobUpdateEvent)
                .cast(JobUpdateEvent.class)
                .materialize()
                .flatMap(notification -> {
                    switch (notification.getType()) {
                        case ON_NEXT:
                            return Flux.just(isJobCompletedEvent(notification.get()));
                        case ON_ERROR:
                            Throwable throwable = notification.getThrowable();
                            if (!(throwable instanceof StatusRuntimeException)) {
                                return Flux.error(Objects.requireNonNull(throwable));
                            }
                            StatusRuntimeException ex = (StatusRuntimeException) throwable;
                            return ex.getStatus().getCode() == Status.Code.NOT_FOUND
                                    ? Flux.just(true)
                                    : Flux.error(throwable);
                    }
                    // case ON_COMPLETED:
                    return Flux.error(new IllegalStateException("Unexpected end of stream for job " + jobId));
                })
                .takeUntil(completed -> completed)
                .ignoreElements()
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(1_000, 30_000, TimeUnit.MILLISECONDS)
                        .withReactorScheduler(Schedulers.parallel())
                        .buildReactorExponentialBackoff()
                )
                .doOnTerminate(() -> {
                    jobCompleted.onComplete();
                    logger.info("Job {} completed", jobId);
                })
                .subscribe();
    }

    @Override
    public Mono<Void> killJob() {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient()
                .killJob(jobId, TEST_CALL_METADATA)
                .onErrorResume(e -> {
                    Status.Code code = Status.fromThrowable(e).getCode();
                    if (code == Status.Code.NOT_FOUND) {
                        logger.info("Tried to kill job {} but it is already gone", jobId);
                        return Mono.empty();
                    }
                    return Mono.error(new IOException("Failed to kill job " + name, e));
                })
                .doOnSuccess(ignored -> logger.info("Killed job {}", jobId));
    }

    @Override
    public Mono<Void> killTask(String taskId) {
        Preconditions.checkState(doRun, "Job executor shut down already");
        Preconditions.checkNotNull(jobId);

        return context.getJobManagementClient()
                .killTask(taskId, false, TEST_CALL_METADATA)
                .onErrorResume(e -> {
                    Status.Code code = Status.fromThrowable(e).getCode();
                    if (code == Status.Code.NOT_FOUND || code == Status.Code.FAILED_PRECONDITION) {
                        logger.info("Tried to kill task {} but it is already gone", taskId);
                        return Mono.empty();
                    }
                    return Mono.error(new IOException(String.format("Failed to kill task %s  of job %s: error=%s", taskId, name, e.getMessage()), e));
                })
                .doOnSuccess(ignored -> logger.info("Killed task {}", taskId));
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

    private boolean isJobCompletedEvent(@Nullable JobUpdateEvent event) {
        if (event == null) {
            return false;
        }
        return event.getCurrent().getStatus().getState() == JobState.Finished;
    }

    private static String buildJobUniqueName(JobDescriptor<?> jobSpec) {
        return jobSpec.getApplicationName() + '-'
                + jobSpec.getJobGroupInfo().getStack() + '-'
                + jobSpec.getJobGroupInfo().getDetail() + '-'
                + jobSpec.getJobGroupInfo().getSequence();
    }
}
