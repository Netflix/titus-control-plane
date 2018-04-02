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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;

public class TextReporter {

    private static final String LINE_FORMAT = "%-10d | %-12d | %-14d | %-14d | %-10d | %-10d | %-15d | %-10d | %-10d";
    private static final String HEADER_FORMAT = "%-10s | %-12s | %-14s | %-14s | %-10s | %-10s | %-15s | %-10s | %-10s";
    private static final String[] HEADERS = {"AllJobs", "ActiveJobs", "PendingIncons", "TotalIncons", "Accepted", "Launched", "StartInitiated", "Started", "KillInitiated"};

    private final MetricsCollector metricsCollector;
    private final Scheduler scheduler;

    private Subscription subscription;

    public TextReporter(MetricsCollector metricsCollector, Scheduler scheduler) {
        this.metricsCollector = metricsCollector;
        this.scheduler = scheduler;
    }

    public void start() {
        System.out.format(HEADER_FORMAT, (Object[]) HEADERS);
        System.out.println();
        this.subscription = Observable.interval(0, 5, TimeUnit.SECONDS, scheduler).subscribe(
                tick -> {
                    Map<TaskState, Long> taskStateCounters = metricsCollector.getActiveTaskStateCounters();
                    System.out.format(LINE_FORMAT,
                            metricsCollector.getTotalSubmittedJobs(),
                            metricsCollector.getActiveJobs(),
                            metricsCollector.getPendingInconsistencies(),
                            metricsCollector.getTotalInconsistencies(),
                            taskStateCounters.getOrDefault(TaskState.Accepted, 0L),
                            taskStateCounters.getOrDefault(TaskState.Launched, 0L),
                            taskStateCounters.getOrDefault(TaskState.StartInitiated, 0L),
                            taskStateCounters.getOrDefault(TaskState.Started, 0L),
                            taskStateCounters.getOrDefault(TaskState.KillInitiated, 0L)
                    );
                    System.out.println();
                }
        );
    }

    public void stop() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }
}
