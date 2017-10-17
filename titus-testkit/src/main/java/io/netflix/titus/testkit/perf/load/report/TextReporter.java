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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;

public class TextReporter {

    private static final AtomicLong ZERO = new AtomicLong();

    private static final String LINE_FORMAT = "%-10d | %-12d | %-14d | %-14d | %-10d | %-10d | %-10d | %-10d | %-10d | %-10d | %-10d | %-10d";
    private static final String HEADER_FORMAT = "%-10s | %-12s | %-14s | %-14s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s";
    private static final String[] HEADERS = {"AllJobs", "ActiveJobs", "PendingIncons", "TotalIncons", "Queued", "Dispatched", "Starting", "Running", "Finished", "Failed", "Crashed", "Error"};

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
                    Map<TitusTaskState, AtomicLong> taskStateCounters = metricsCollector.getActiveTaskStateCounters();
                    System.out.format(LINE_FORMAT,
                            metricsCollector.getTotalSubmittedJobs(),
                            metricsCollector.getActiveJobs(),
                            metricsCollector.getPendingInconsistencies(),
                            metricsCollector.getTotalInconsistencies(),
                            taskStateCounters.getOrDefault(TitusTaskState.QUEUED, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.DISPATCHED, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.STARTING, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.RUNNING, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.FINISHED, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.FAILED, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.CRASHED, ZERO).get(),
                            taskStateCounters.getOrDefault(TitusTaskState.ERROR, ZERO).get()
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
