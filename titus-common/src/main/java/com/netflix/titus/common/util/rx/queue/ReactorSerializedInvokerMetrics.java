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

package com.netflix.titus.common.util.rx.queue;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;

class ReactorSerializedInvokerMetrics {

    private static final String ROOT_NAME = "titus.common.serializedInvoker.";

    private final Counter submitCounter;
    private final Counter queueFullCounter;
    private final Gauge queueSize;

    private final Timer queueingTimer;
    private final Timer executionTimer;
    private final Timer executionErrorTimer;
    private final Timer executionDisposedTimer;

    private final String name;
    private final Registry registry;

    ReactorSerializedInvokerMetrics(String name, Registry registry) {
        this.name = name;
        this.registry = registry;

        this.submitCounter = registry.counter(ROOT_NAME + "submit", "name", name);
        this.queueFullCounter = registry.counter(ROOT_NAME + "queueFull", "name", name);
        this.queueSize = registry.gauge(ROOT_NAME + "queueSize", "name", name);

        this.queueingTimer = registry.timer(ROOT_NAME + "queueingTime", "name", name);
        this.executionTimer = registry.timer(ROOT_NAME + "executionTime", "name", name, "status", "success");
        this.executionErrorTimer = registry.timer(ROOT_NAME + "executionTime", "name", name, "status", "error");
        this.executionDisposedTimer = registry.timer(ROOT_NAME + "executionTime", "name", name, "status", "disposed");
    }

    void onSubmit() {
        submitCounter.increment();
    }

    void setQueueSize(int size) {
        queueSize.set(size);
    }

    void onQueueFull() {
        queueFullCounter.increment();
    }

    void onActionExecutionStarted(long queueingTimeMs) {
        queueingTimer.record(queueingTimeMs, TimeUnit.MILLISECONDS);
    }

    void onActionCompleted(long executionTimeMs) {
        executionTimer.record(executionTimeMs, TimeUnit.MILLISECONDS);
    }

    void onActionFailed(long executionTimeMs, Throwable error) {
        executionErrorTimer.record(executionTimeMs, TimeUnit.MILLISECONDS);
        registry.counter(ROOT_NAME + "executionError",
                "name", name, "error", error.getClass().getSimpleName()
        ).increment();
    }

    void onActionExecutionDisposed(long executionTimeMs) {
        executionDisposedTimer.record(executionTimeMs, TimeUnit.MILLISECONDS);
    }
}
