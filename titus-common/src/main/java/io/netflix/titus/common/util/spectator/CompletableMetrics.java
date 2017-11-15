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
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.LongTaskTimer;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import rx.Completable;

/**
 * RxJava completable metrics.
 */
class CompletableMetrics implements Completable.Transformer {

    private final Registry registry;

    private final Id rootId;
    private final LongTaskTimer duration;
    private final AtomicLong timerId;
    private final AtomicLong lastCompleteTimestamp;

    CompletableMetrics(String root, List<Tag> commonTags, Registry registry) {
        this.rootId = registry.createId(root, commonTags);
        this.registry = registry;
        this.duration = registry.longTaskTimer(root + ".duration", commonTags);
        this.timerId = new AtomicLong(0);
        this.lastCompleteTimestamp = new AtomicLong(registry.clock().wallTime());
        registry.gauge(
                registry.createId(root + ".timeSinceLastComplete", commonTags),
                0L,
                this::getTimeSinceLastComplete
        );
    }

    @Override
    public Completable call(Completable completable) {
        return completable.doOnSubscribe(subscription -> timerId.getAndSet(duration.start()))
                .doOnError(e -> registry.gauge(rootId.withTags("status", "error", "error", e.getClass().getSimpleName()), 1))
                .doOnCompleted(() -> {
                    registry.gauge(rootId.withTag("status", "success"), 1);
                    duration.stop(timerId.get());
                    lastCompleteTimestamp.getAndSet(registry.clock().wallTime());
                });
    }

    private long getTimeSinceLastComplete(long value) {
        return registry.clock().wallTime() - lastCompleteTimestamp.get();
    }
}
