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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import rx.Completable;

/**
 * This is a transformer that instruments a continuously scheduled completable with metrics in order to determine the latency
 * of the completable as well as how long it has been since the completable last completed. This is useful
 * for completables that consistently get scheduled but has the following limitations:
 * <ul>
 * <li>This class only measures the latency based on the first subscription until the first complete.</li>
 * <li>The same instance of this transformer must be used in order to keep the complete timestamp history.</li>
 * </ul>
 */
public class ScheduledCompletableMetrics implements Completable.Transformer {
    private final Registry registry;
    private final Timer latency;
    private final AtomicBoolean shouldRemove;
    private final AtomicBoolean hasSubscribed;
    private final AtomicLong latencyStart;
    private final AtomicLong lastCompleteTimestamp;
    private final Id timeSinceLastCompleteId;

    ScheduledCompletableMetrics(String root, List<Tag> commonTags, Registry registry) {
        this.registry = registry;
        this.latency = registry.timer(root + ".latency", commonTags);
        this.shouldRemove = new AtomicBoolean(false);
        this.hasSubscribed = new AtomicBoolean(false);
        this.latencyStart = new AtomicLong(0);
        this.lastCompleteTimestamp = new AtomicLong(registry.clock().wallTime());
        this.timeSinceLastCompleteId = registry.createId(root + ".timeSinceLastComplete", commonTags);

        PolledMeter.using(registry)
                .withId(timeSinceLastCompleteId)
                .monitorValue(this, ScheduledCompletableMetrics::getTimeSinceLastComplete);
    }

    @Override
    public Completable call(Completable completable) {
        return completable
                .doOnSubscribe(subscription -> {
                    if (!shouldRemove.get() && !hasSubscribed.get()) {
                        hasSubscribed.set(true);
                        latencyStart.set(registry.clock().wallTime());
                    }
                })
                .doOnCompleted(() -> {
                    if (!shouldRemove.get()) {
                        final long end = registry.clock().wallTime();
                        latency.record(end - latencyStart.get(), TimeUnit.MILLISECONDS);
                        lastCompleteTimestamp.set(registry.clock().wallTime());
                        hasSubscribed.set(false);
                    }
                });
    }

    public void remove() {
        shouldRemove.set(true);
        PolledMeter.remove(registry, timeSinceLastCompleteId);
    }

    private long getTimeSinceLastComplete() {
        if (shouldRemove.get()) {
            return 0;
        }
        return registry.clock().wallTime() - lastCompleteTimestamp.get();
    }
}
