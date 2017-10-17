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

package io.netflix.titus.testkit.perf.load.report;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent.ConsistencyRestoreEvent;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent.InconsistentStateEvent;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent.JobFinishedEvent;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent.JobSubmitEvent;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent.TaskCreateEvent;
import io.netflix.titus.testkit.perf.load.job.JobChangeEvent.TaskStateChangeEvent;
import rx.Observable;
import rx.Subscription;

public class MetricsCollector {

    private final AtomicLong totalSubmittedJobs = new AtomicLong();
    private final AtomicLong totalInconsistencies = new AtomicLong();
    private final ConcurrentMap<TitusJobState, AtomicLong> totalJobStatusCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<TitusTaskState, AtomicLong> totalTaskStateCounters = new ConcurrentHashMap<>();

    private final AtomicLong activeJobs = new AtomicLong();
    private final Set<String> pendingInconsistentJobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ConcurrentMap<TitusTaskState, AtomicLong> activeTaskStateCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TitusTaskState> activeTaskLastStates = new ConcurrentHashMap<>();

    private Subscription subscription;

    public long getTotalSubmittedJobs() {
        return totalSubmittedJobs.get();
    }

    public long getActiveJobs() {
        return activeJobs.get();
    }

    public ConcurrentMap<TitusTaskState, AtomicLong> getActiveTaskStateCounters() {
        return activeTaskStateCounters;
    }

    public long getTotalInconsistencies() {
        return totalInconsistencies.get();
    }

    public int getPendingInconsistencies() {
        return pendingInconsistentJobs.size();
    }

    public Map<TitusJobState, AtomicLong> getTotalJobStatusCounters() {
        return totalJobStatusCounters;
    }

    public Map<TitusTaskState, AtomicLong> getTotalTaskStateCounters() {
        return totalTaskStateCounters;
    }

    public void watch(Observable<JobChangeEvent> eventObservable) {
        Preconditions.checkState(subscription == null);

        this.subscription = eventObservable.subscribe(
                event -> {
                    if (event instanceof JobSubmitEvent) {
                        totalSubmittedJobs.incrementAndGet();
                        activeJobs.incrementAndGet();
                    } else if (event instanceof JobFinishedEvent) {
                        JobFinishedEvent je = (JobFinishedEvent) event;
                        totalJobStatusCounters.computeIfAbsent(je.getState(), s -> new AtomicLong()).incrementAndGet();
                        activeJobs.decrementAndGet();
                        pendingInconsistentJobs.remove(je.getJobId());
                    } else if (event instanceof TaskCreateEvent) {
                        TaskCreateEvent te = (TaskCreateEvent) event;
                        totalTaskStateCounters.computeIfAbsent(te.getTaskState(), s -> new AtomicLong()).incrementAndGet();

                        if (te.getTaskState().isActive()) {
                            activeTaskStateCounters.computeIfAbsent(te.getTaskState(), s -> new AtomicLong()).incrementAndGet();
                            activeTaskLastStates.put(te.getTaskId(), te.getTaskState());
                        }
                    } else if (event instanceof TaskStateChangeEvent) {
                        TaskStateChangeEvent te = (TaskStateChangeEvent) event;
                        totalTaskStateCounters.computeIfAbsent(te.getTaskState(), s -> new AtomicLong()).incrementAndGet();

                        // First remove count for the previous state
                        TitusTaskState lastState = activeTaskLastStates.get(te.getTaskId());
                        if (lastState != null) {
                            AtomicLong counter = activeTaskStateCounters.get(lastState);
                            if (counter != null) {
                                counter.decrementAndGet();
                            }
                        }

                        // Now account for the new state
                        if (te.getTaskState().isActive()) {
                            activeTaskStateCounters.computeIfAbsent(te.getTaskState(), s -> new AtomicLong()).incrementAndGet();
                            activeTaskLastStates.put(te.getTaskId(), te.getTaskState());
                        } else {
                            activeTaskLastStates.remove(te.getTaskId());
                        }
                    } else if (event instanceof InconsistentStateEvent) {
                        if (!pendingInconsistentJobs.contains(event.getJobId())) {
                            pendingInconsistentJobs.add(event.getJobId());
                            totalInconsistencies.incrementAndGet();
                        }
                    } else if (event instanceof ConsistencyRestoreEvent) {
                        pendingInconsistentJobs.remove(event.getJobId());
                    }
                }
        );
    }
}
