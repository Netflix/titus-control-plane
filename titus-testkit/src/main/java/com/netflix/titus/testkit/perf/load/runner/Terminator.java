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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Removes running jobs belonging to past sessions.
 */
@Singleton
public class Terminator {

    private static final Logger logger = LoggerFactory.getLogger(Terminator.class);

    private final ExecutionContext context;

    @Inject
    public Terminator(ExecutionContext context) {
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

            List<Observable<Void>> killActions = jobIdsToRemove.stream()
                    .map(jid -> context.getJobManagementClient()
                            .killJob(jid)
                            .onErrorResumeNext(e -> {
                                StatusRuntimeException ex = (StatusRuntimeException) e;
                                return ex.getStatus().getCode() == Status.Code.NOT_FOUND
                                        ? Observable.empty()
                                        : Observable.error(e);

                            })
                    )
                    .collect(Collectors.toList());
            try {
                Observable.mergeDelayError(killActions, 10).toBlocking().firstOrDefault(null);
            } catch (Throwable e) {
                logger.warn("Not all jobs successfully terminated", e);
            }
        }
    }

    private boolean doTry(Set<String> jobIdsToRemove, Set<String> unknownJobs) {
        try {
            Iterator<JobChangeNotification> it = context.getJobManagementClientBlocking().observeJobs(Empty.getDefaultInstance());
            while (it.hasNext()) {
                JobChangeNotification event = it.next();
                if (event.getNotificationCase() == JobChangeNotification.NotificationCase.SNAPSHOTEND) {
                    break;
                }
                if (event.getNotificationCase() != JobChangeNotification.NotificationCase.JOBUPDATE) {
                    continue;
                }
                Job job = event.getJobUpdate().getJob();
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
        return job.getStatus().getState() == JobStatus.JobState.Accepted
                && job.getJobDescriptor().getAttributesMap().containsKey(ExecutionContext.LABEL_SESSION);
    }
}
