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

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import rx.Completable;

/**
 * This is a transformer that instruments a long running completable with metrics in order to determine the duration
 * of the completable as well as how long it has been since the completable last completed. This is useful
 * for long running tasks but has the following limitations:
 * <ul>
 * <li>This class only measures the duration based on the first subscription until the first complete.</li>
 * <li>The same instance of this transformer must be used in order to keep the complete timestamp history.</li>
 * </ul>
 */
class LongRunningCompletableMetrics implements Completable.Transformer {

    private final Registry registry;

    private final Timer duration;
    private final AtomicBoolean hasSubscribed;
    private final AtomicLong durationStart;
    private final AtomicLong lastCompleteTimestamp;

    LongRunningCompletableMetrics(String root, List<Tag> commonTags, Registry registry) {
        this.registry = registry;
        this.duration = registry.timer(root + ".duration", commonTags);
        this.hasSubscribed = new AtomicBoolean(false);
        this.durationStart = new AtomicLong(0);
        this.lastCompleteTimestamp = new AtomicLong(registry.clock().wallTime());
        registry.gauge(
                registry.createId(root + ".timeSinceLastComplete", commonTags),
                0L,
                this::getTimeSinceLastComplete
        );
    }

    @Override
    public Completable call(Completable completable) {
        return completable
                .doOnSubscribe(subscription -> {
                    if (!hasSubscribed.get()) {
                        hasSubscribed.getAndSet(true);
                        durationStart.getAndSet(registry.clock().wallTime());
                    }
                })
                .doOnCompleted(() -> {
                    final long end = registry.clock().monotonicTime();
                    duration.record(end - durationStart.get(), TimeUnit.MILLISECONDS);
                    lastCompleteTimestamp.getAndSet(registry.clock().wallTime());
                    hasSubscribed.getAndSet(false);
                });
    }

    private long getTimeSinceLastComplete(long value) {
        return registry.clock().wallTime() - lastCompleteTimestamp.get();
    }
}
