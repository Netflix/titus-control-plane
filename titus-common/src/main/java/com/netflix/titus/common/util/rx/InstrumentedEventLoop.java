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

package com.netflix.titus.common.util.rx;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.util.spectator.ActionMetrics;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import rx.Scheduler;
import rx.functions.Action0;

/**
 * This is an instrumented event loop that takes actions and runs them sequentially on the given scheduler.
 */
public class InstrumentedEventLoop {

    private final String metricNameRoot;
    private final Registry registry;
    private final Scheduler.Worker worker;
    private final Map<String, ActionMetrics> actionMetrics;
    private final AtomicLong actionsRemaining;
    private final Id actionsRemainingId;

    public InstrumentedEventLoop(String metricNameRoot, Registry registry, Scheduler scheduler) {
        this.metricNameRoot = metricNameRoot;
        this.registry = registry;
        this.worker = scheduler.createWorker();
        this.actionMetrics = new ConcurrentHashMap<>();
        this.actionsRemaining = new AtomicLong(0);
        this.actionsRemainingId = registry.createId(metricNameRoot + ".eventLoop.actionsRemaining");

        PolledMeter.using(registry)
                .withId(this.actionsRemainingId)
                .monitorValue(this.actionsRemaining);
    }

    public void schedule(String actionName, Action0 action) {
        worker.schedule(instrumentedAction(actionName, action));
        actionsRemaining.incrementAndGet();
    }

    public void schedule(String actionName, Action0 action, long delayTime, TimeUnit unit) {
        worker.schedule(instrumentedAction(actionName, action), delayTime, unit);
        actionsRemaining.incrementAndGet();
    }

    public void shutdown() {
        if (!worker.isUnsubscribed()) {
            worker.unsubscribe();
        }
        actionMetrics.clear();
        PolledMeter.remove(registry, actionsRemainingId);
    }

    private Action0 instrumentedAction(String actionName, Action0 action) {
        return () -> {
            ActionMetrics actionMetrics = this.actionMetrics.computeIfAbsent(actionName, k -> {
                String rootName = metricNameRoot + ".eventLoop." + actionName;
                return SpectatorExt.actionMetrics(rootName, Collections.emptyList(), registry);
            });
            long start = actionMetrics.start();
            try {
                action.call();
                actionMetrics.success();
            } catch (Exception e) {
                actionMetrics.failure(e);
            } finally {
                actionMetrics.finish(start);
                actionsRemaining.decrementAndGet();
            }
        };
    }
}
