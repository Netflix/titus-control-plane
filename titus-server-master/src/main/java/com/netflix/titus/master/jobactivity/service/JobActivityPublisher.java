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

package com.netflix.titus.master.jobactivity.service;

import java.util.function.Predicate;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.titus.api.FeatureRolloutPlans;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherStore;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.SchedulerExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The JobActivityPublisher consumes job and task events from the V3 engine
 * and immediately propagates them to a store.
 */
@Singleton
public class JobActivityPublisher {
    private static final Logger logger = LoggerFactory.getLogger(JobActivityPublisher.class);

    private static final String JOB_ACTIVITY_PUBLISHER_SCHEDULER = "JobActivityPublisherScheduler";

    private final JobActivityPublisherStore publisher;
    private final V3JobOperations v3JobOperations;
    private Disposable jobUpdateEventDisposable;

    private final TitusRuntime runtime;
    private final JobActivityPublisherMetrics metrics;
    private final int jobActivityStreamBufferSize;

    private final Predicate<Job> jobActivityPublisherEnabledPredicate;

    @Inject
    public JobActivityPublisher(JobActivityPublisherConfiguration configuration,
                                JobActivityPublisherStore publisher,
                                V3JobOperations v3JobOperations,
                                @Named(FeatureRolloutPlans.JOB_ACTIVITY_PUBLISH_FEATURE) Predicate<JobDescriptor> jobActivityPublishPredicate,
                                TitusRuntime runtime) {
        this.publisher = publisher;
        this.v3JobOperations = v3JobOperations;
        this.jobActivityPublisherEnabledPredicate = job -> jobActivityPublishPredicate.test(job.getJobDescriptor());
        this.runtime = runtime;
        this.metrics = new JobActivityPublisherMetrics(runtime.getRegistry());
        this.jobActivityStreamBufferSize = configuration.getJobActivityPublisherMaxStreamSize();
    }

    @Activator
    public void activate() {
        logger.info("Starting job activity publisher");
        jobUpdateEventDisposable = jobManagerStream().subscribe(
                this::handleJobManagerEvent,
                e -> {
                    logger.error("Error in job activity publisher stream: ", e);
                },
                () -> {
                    logger.error("Unexpected completion of job activity publisher stream");
                }
        );
    }

    @Deactivator
    public void deactivate() {
        jobUpdateEventDisposable.dispose();
    }

    public boolean isActive() {
        return !jobUpdateEventDisposable.isDisposed();
    }

    private Flux<JobManagerEvent<?>> jobManagerStream() {
        Scheduler scheduler = Schedulers.newSingle(JOB_ACTIVITY_PUBLISHER_SCHEDULER, true);
        // The TitusRuntime emits stream metrics so we avoid explicitly managing them here
        return ReactorExt.toFlux(runtime.persistentStream(v3JobOperations.observeJobs()))
                .onBackpressureBuffer(jobActivityStreamBufferSize,
                        jobManagerEvent -> {
                            metrics.publishDropError();
                            logger.warn("Dropping events due to back pressure buffer of size {} overflow",
                                    jobActivityStreamBufferSize);
                        },
                        BufferOverflowStrategy.DROP_LATEST)
                .subscribeOn(scheduler);
    }

    /**
     * Handles sending JobManagerEvents to the publisher. If errors are encountered we are
     * currently emitting metrics to alert on but continuing to publish new events. Once we
     * move publishing of events to be inline with the user request, we should handle the error and
     * potentially fail the user request.
     * Without failing the job creation or being able to reconcile missed events, errors to publish
     * events will simply result in missed records being published to Job Activity.
     */
    private void handleJobManagerEvent(JobManagerEvent<?> jobManagerEvent) {
        try {
            if (jobManagerEvent instanceof JobUpdateEvent) {
                JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) jobManagerEvent;
                handleJobUpdateEvent(jobUpdateEvent);
            } else if (jobManagerEvent instanceof TaskUpdateEvent) {
                TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) jobManagerEvent;
                handleTaskUpdateEvent(taskUpdateEvent);
            } else {
                logger.error("Got other event type: {} on {}", jobManagerEvent.getClass(), Thread.currentThread().getId());
            }
        } catch (Exception e) {
            // Paranoid check to avoid Exception propagation to the event stream publisher
            logger.error("Exception caught while processing event stream. DROPPING to avoid impacting publisher: {}", e);
        }
    }

    private void handleJobUpdateEvent(JobUpdateEvent jobUpdateEvent) {
        Job<?> job = jobUpdateEvent.getCurrent();
        if (!jobActivityPublisherEnabledPredicate.test(job)) {
            logger.debug("Skipping job activity publish because feature is disabled for this job");
            return;
        }

        publisher.publishJob(job).subscribe(
                voidResult -> {},
                e -> {
                    metrics.publishError(JobActivityPublisherRecord.RecordType.JOB, job.getId(), e);
                    logger.error("Failed to publish update for job {}: {}", job.getId(), e);
                },
                () -> {
                    metrics.publishSuccess(JobActivityPublisherRecord.RecordType.JOB);
                    logger.debug("Successfully published update for job {}", job.getId());
                }
        );
    }

    private void handleTaskUpdateEvent(TaskUpdateEvent taskUpdateEvent) {
        Job<?> job = taskUpdateEvent.getCurrentJob();
        Task task = taskUpdateEvent.getCurrentTask();
        if (!jobActivityPublisherEnabledPredicate.test(job)) {
            logger.debug("Skipping job activity publish because feature is disabled for this task's job");
            return;
        }

        publisher.publishTask(task).subscribe(
                voidResult -> {},
                e -> {
                    metrics.publishError(JobActivityPublisherRecord.RecordType.TASK, task.getId(), e);
                    logger.error("Failed to publish update for task {}: {}",task.getId(), e);
                },
                () -> {
                    metrics.publishSuccess(JobActivityPublisherRecord.RecordType.TASK);
                    logger.debug("Successfully published update for task {}", task.getId());
                }
        );
    }
}
