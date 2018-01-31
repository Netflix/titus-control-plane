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

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

public abstract class AbstractJobExecutor<E extends JobDescriptor.JobDescriptorExt> implements JobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobExecutor.class);

    protected final JobDescriptor<?> jobSpec;
    private final Observable<JobManagerEvent<?>> allJobsChangeObservable;
    protected final ExecutionContext context;

    protected volatile String name;
    protected volatile boolean doRun = true;

    protected volatile String jobId;
    protected volatile Job<E> job;
    protected volatile List<Task> activeTasks = Collections.emptyList();

    protected volatile Subscription activeJobsSubscription;

    protected AbstractJobExecutor(JobDescriptor<E> jobSpec,
                                  Observable<JobManagerEvent<?>> allJobsChangeObservable,
                                  ExecutionContext context) {
        this.jobSpec = jobSpec;
        this.allJobsChangeObservable = allJobsChangeObservable;
        this.context = context;
        this.name = buildJobUniqueName(jobSpec);
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
    public Observable<JobManagerEvent<?>> updates() {
        return allJobsChangeObservable
                .filter(event -> {
                    if (event instanceof JobUpdateEvent) {
                        return ((JobUpdateEvent) event).getCurrent().getId().equals(jobId);
                    }
                    return ((TaskUpdateEvent) event).getCurrent().getJobId().equals(jobId);
                })
                .takeUntil(this::isJobCloseEvent);
    }

    private boolean isJobCloseEvent(JobManagerEvent<?> event) {
        if (event instanceof JobUpdateEvent) {
            return ((JobUpdateEvent) event).getCurrent().getStatus().getState() == JobState.Finished;
        }
        return false;
    }

    @Override
    public void shutdown() {
        if (doRun) {
            doRun = false;
            if (activeJobsSubscription != null) {
                this.activeTasks = Collections.emptyList();
                activeJobsSubscription.unsubscribe();

                Throwable error = context.getJobManagementClient().killJob(jobId).toCompletable().get();
                if (error != null) {
                    logger.debug("Job {} cleanup failure", jobId, error);
                }
            }
        }
    }

    @Override
    public Observable<Void> submitJob() {
        Preconditions.checkState(doRun, "Job executor shut down already");
        return context.getJobManagementClient()
                .createJob(V3GrpcModelConverters.toGrpcJobDescriptor(jobSpec))
                .flatMap(jobRef -> context.getJobManagementClient().findJob(jobRef))
                .doOnNext(job -> {
                    this.jobId = job.getId();
                    this.job = V3GrpcModelConverters.toCoreJob(job);
                    logger.info("Submitted job {}", jobId);
                    this.name = name + '(' + jobId + ')';
                })
                .ignoreElements()
                .cast(Void.class)
                .onErrorResumeNext(e -> Observable.error(new IOException("Failed to submit job " + name, e)));
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

    private static String buildJobUniqueName(JobDescriptor<?> jobSpec) {
        return jobSpec.getApplicationName() + '-'
                + jobSpec.getJobGroupInfo().getStack() + '-'
                + jobSpec.getJobGroupInfo().getDetail() + '-'
                + jobSpec.getJobGroupInfo().getSequence();
    }
}
