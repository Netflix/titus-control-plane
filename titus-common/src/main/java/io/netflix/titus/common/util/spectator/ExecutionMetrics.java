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

package io.netflix.titus.common.util.spectator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.netflix.titus.common.util.CollectionsExt;

/**
 * A collection of metrics for tracking successful and failed action executions.
 */
public class ExecutionMetrics {
    private final String root;
    private final Registry registry;
    private final List<Tag> commonTags;

    private final Counter successCounter;
    private final Counter errorCounter;
    private final Map<Class<? extends Throwable>, Counter> exceptionCounters = new ConcurrentHashMap<>();

    private final AtomicLong successLatency = new AtomicLong();
    private final AtomicLong failureLatency = new AtomicLong();

    public ExecutionMetrics(String root, Class<?> aClass, Registry registry) {
        this(root, aClass, registry, new ArrayList<>());
    }

    public ExecutionMetrics(String root, Class<?> aClass, Registry registry, List<Tag> additionalTags) {
        this.root = root;
        this.registry = registry;
        this.commonTags = CollectionsExt.copyAndAdd(additionalTags, new BasicTag("class", aClass.getName()));

        this.successCounter = registry.counter(registry.createId(root, commonTags).withTag("status", "success"));
        this.errorCounter = registry.counter(registry.createId(root, commonTags).withTag("status", "failure"));

        Id successLatencyId = registry.createId(root + ".latency", commonTags).withTag("status", "success");
        PolledMeter.using(registry).withId(successLatencyId).monitorValue(successLatency);
        Id failureLatencyId = registry.createId(root + ".latency", commonTags).withTag("status", "failure");
        PolledMeter.using(registry).withId(failureLatencyId).monitorValue(failureLatency);
    }

    public void success() {
        successCounter.increment();
    }

    public void success(long startTime) {
        success();
        successLatency.set(registry.clock().wallTime() - startTime);
        failureLatency.set(0);
    }

    public void failure(Throwable error) {
        Counter exceptionCounter = exceptionCounters.get(error.getClass());
        if (exceptionCounter == null) {
            Id errorId = registry.createId(root, commonTags).withTag("exception", error.getClass().getSimpleName());
            exceptionCounter = registry.counter(errorId);
            exceptionCounters.put(error.getClass(), exceptionCounter);
        }

        errorCounter.increment();
        exceptionCounter.increment();
    }

    public void failure(Throwable error, long startTime) {
        failure(error);
        successLatency.set(0);
        failureLatency.set(registry.clock().wallTime() - startTime);
    }
}
