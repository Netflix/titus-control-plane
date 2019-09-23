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

package com.netflix.titus.common.util.spectator;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;

/**
 * A collection of metrics for tracking successful and failed action executions.
 */
public class ActionMetrics implements Closeable {
    private final String root;
    private final Registry registry;
    private final Iterable<Tag> commonTags;

    private final Counter successCounter;
    private final Map<Class<? extends Throwable>, Counter> exceptionCounters = new ConcurrentHashMap<>();

    private final Timer latencyTimerOnSuccess;
    private final Timer latencyTimerOnError;
    private final Id sinceLastExecutionId;
    private final AtomicLong lastCompletionRef;

    public ActionMetrics(Id rootId, Registry registry) {
        this.root = rootId.name();
        this.commonTags = rootId.tags();
        this.registry = registry;
        this.successCounter = registry.counter(registry.createId(root, commonTags).withTag("status", "success"));

        this.latencyTimerOnSuccess = registry.timer(registry.createId(root + ".latency", commonTags).withTag("status", "success"));
        this.latencyTimerOnError = registry.timer(registry.createId(root + ".latency", commonTags).withTag("status", "failure"));

        this.sinceLastExecutionId = registry.createId(root + ".sinceLastCompletion", commonTags);
        this.lastCompletionRef = new AtomicLong(registry.clock().wallTime());
        PolledMeter.using(registry).withId(sinceLastExecutionId).monitorValue(lastCompletionRef);
    }

    @Override
    public void close() {
        PolledMeter.remove(registry, sinceLastExecutionId);
    }

    public long start() {
        return registry.clock().wallTime();
    }

    public void success() {
        successCounter.increment();
        lastCompletionRef.set(registry.clock().wallTime());
    }

    public void finish(long startTime) {
        success();
        latencyTimerOnSuccess.record(registry.clock().wallTime() - startTime, TimeUnit.MILLISECONDS);
    }

    public void failure(Throwable error) {
        failure(-1, error);
    }

    public void failure(long startTime, Throwable error) {
        lastCompletionRef.set(registry.clock().wallTime());
        if (startTime >= 0) {
            latencyTimerOnError.record(registry.clock().wallTime() - startTime, TimeUnit.MILLISECONDS);
        }

        Counter exceptionCounter = exceptionCounters.get(error.getClass());
        if (exceptionCounter == null) {
            Id errorId = registry.createId(root, commonTags)
                    .withTag("status", "failure")
                    .withTag("exception", error.getClass().getSimpleName());
            exceptionCounter = registry.counter(errorId);
            exceptionCounters.put(error.getClass(), exceptionCounter);
        }
        exceptionCounter.increment();
    }
}
