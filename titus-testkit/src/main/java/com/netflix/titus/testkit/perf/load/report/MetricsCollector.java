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

package com.netflix.titus.testkit.perf.load.report;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.testkit.client.V3ClientUtils;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

public class MetricsCollector {

    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    private final AtomicLong totalSubmittedJobs = new AtomicLong();
    private final AtomicLong totalInconsistencies = new AtomicLong();
    private final ConcurrentMap<JobState, AtomicLong> totalJobStatusCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskState, AtomicLong> totalTaskStateCounters = new ConcurrentHashMap<>();

    private final AtomicLong activeJobs = new AtomicLong();
    private final Set<String> pendingInconsistentJobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ConcurrentMap<String, TaskState> activeTaskLastStates = new ConcurrentHashMap<>();

    private Subscription subscription;

    public long getTotalSubmittedJobs() {
        return totalSubmittedJobs.get();
    }

    public long getActiveJobs() {
        return activeJobs.get();
    }

    public Map<TaskState, Long> getActiveTaskStateCounters() {
        Map<TaskState, Long> stateCounts = new HashMap<>();
        activeTaskLastStates.values().forEach(state -> stateCounts.put(state, stateCounts.getOrDefault(state, 0L) + 1));
        return stateCounts;
    }

    public long getTotalInconsistencies() {
        return totalInconsistencies.get();
    }

    public int getPendingInconsistencies() {
        return pendingInconsistentJobs.size();
    }

    public Map<JobState, AtomicLong> getTotalJobStatusCounters() {
        return totalJobStatusCounters;
    }

    public Map<TaskState, AtomicLong> getTotalTaskStateCounters() {
        return totalTaskStateCounters;
    }

    public void watch(ExecutionContext context) {
        Preconditions.checkState(subscription == null);

        this.subscription = Observable.defer(() -> V3ClientUtils.observeJobs(context.getJobServiceGateway().observeJobs(ObserveJobsQuery.getDefaultInstance())))
                .doOnNext(event -> {
                    if (event instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdate = (JobUpdateEvent) event;
                        Job<?> current = jobUpdate.getCurrent();
                        totalJobStatusCounters.computeIfAbsent(current.getStatus().getState(), s -> new AtomicLong()).incrementAndGet();

                        if (!jobUpdate.getPrevious().isPresent()) {
                            totalSubmittedJobs.incrementAndGet();
                            activeJobs.incrementAndGet();
                        } else {
                            if (current.getStatus().getState() == JobState.Finished) {
                                activeJobs.decrementAndGet();
                                pendingInconsistentJobs.remove(current.getId());
                            }
                        }
                    } else {
                        TaskUpdateEvent taskUpdate = (TaskUpdateEvent) event;
                        Task current = taskUpdate.getCurrent();
                        activeTaskLastStates.put(current.getId(), current.getStatus().getState());
                        totalTaskStateCounters.computeIfAbsent(current.getStatus().getState(), s -> new AtomicLong()).incrementAndGet();
                    }
                })
                .ignoreElements()
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(1_000, 30_000, TimeUnit.MILLISECONDS)
                        .withOnErrorHook(e -> {
                            logger.warn("Job subscription error. Re-subscribing and recomputing active jobs metrics...");
                            this.activeJobs.set(0);
                            this.activeTaskLastStates.clear();
                        })
                        .buildExponentialBackoff()
                )
                .subscribe();
    }
}
