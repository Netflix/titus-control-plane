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

package com.netflix.titus.testkit.perf.load.runner;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Removes running jobs belonging to past sessions.
 */
@Singleton
public class JobTerminator {

    private static final Logger logger = LoggerFactory.getLogger(JobTerminator.class);

    private final ExecutionContext context;

    @Inject
    public JobTerminator(ExecutionContext context) {
        this.context = context;
    }

    public void doClean() {
        Set<String> jobIdsToRemove = new HashSet<>();
        Set<String> unknownJobs = new HashSet<>();
        while (!doTry(jobIdsToRemove, unknownJobs)) {
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
            }
        }

        if (!unknownJobs.isEmpty()) {
            logger.warn("There are identified jobs running, which will not be removed: {}", unknownJobs);
        }
        if (!jobIdsToRemove.isEmpty()) {
            logger.warn("Removing old jobs: {}", jobIdsToRemove);

            List<Mono<Void>> killActions = jobIdsToRemove.stream()
                    .map(jid -> context.getJobManagementClient()
                            .killJob(jid, JobManagerConstants.JOB_TERMINATOR_CALL_METADATA)
                            .onErrorResume(e -> {
                                if (!(e instanceof StatusRuntimeException)) {
                                    return Mono.error(e);
                                }
                                StatusRuntimeException ex = (StatusRuntimeException) e;
                                return ex.getStatus().getCode() == Status.Code.NOT_FOUND
                                        ? Mono.empty()
                                        : Mono.error(e);
                            })
                    )
                    .collect(Collectors.toList());
            try {
                Flux.mergeDelayError(10, killActions.stream().toArray(Mono[]::new)).blockLast();
            } catch (Throwable e) {
                logger.warn("Not all jobs successfully terminated", e);
            }
        }
    }

    private boolean doTry(Set<String> jobIdsToRemove, Set<String> unknownJobs) {
        try {
            Iterable<JobManagerEvent<?>> it = context.getJobManagementClient()
                    .observeJobs(Collections.emptyMap())
                    .toIterable();
            for (JobManagerEvent<?> event : it) {
                if (event.equals(JobManagerEvent.snapshotMarker())) {
                    break;
                }
                if (!(event instanceof JobUpdateEvent)) {
                    continue;
                }
                JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                Job job = jobUpdateEvent.getCurrent();
                if (isActivePreviousSessionJob(job)) {
                    jobIdsToRemove.add(job.getId());
                } else {
                    unknownJobs.add(job.getId());
                }
            }
            return true;
        } catch (Throwable e) {
            logger.warn("Could not load active jobs from TitusMaster: {}", ExceptionExt.toMessageChain(e));
            return false;
        }
    }

    private boolean isActivePreviousSessionJob(Job job) {
        return job.getStatus().getState() == JobState.Accepted
                && job.getJobDescriptor().getAttributes().containsKey(ExecutionContext.LABEL_SESSION);
    }
}
