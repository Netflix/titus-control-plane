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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import rx.Observable;

/**
 * RxJava subscription metrics.
 */
class SubscriptionMetrics<T> implements Observable.Transformer<T, T> {

    private final Registry registry;

    private final Id rootId;
    private final Counter onNextCounter;
    private final AtomicInteger unsubscribed;
    private final AtomicLong lastEmitTimestamp;

    SubscriptionMetrics(String root, Class<?> aClass, Registry registry) {
        this(root, Collections.singletonList(new BasicTag("class", aClass.getSimpleName())), registry);
    }

    SubscriptionMetrics(String root, List<Tag> commonTags, Registry registry) {
        this.rootId = registry.createId(root, commonTags);
        this.registry = registry;
        this.onNextCounter = registry.counter(root + ".onNext", commonTags);
        this.unsubscribed = registry.gauge(rootId.withTag("status", "unsubscribed"), new AtomicInteger());
        this.lastEmitTimestamp = new AtomicLong(registry.clock().wallTime());
        registry.gauge(
                registry.createId(root + ".lastEmit", commonTags),
                0L,
                this::getTimeSinceLastEmit
        );
    }

    @Override
    public Observable<T> call(Observable<T> source) {
        return source
                .doOnNext(next -> {
                    onNextCounter.increment();
                    lastEmitTimestamp.set(registry.clock().wallTime());
                })
                .doOnError(e -> registry.gauge(rootId.withTags("status", "error", "error", e.getClass().getSimpleName()), 1))
                .doOnCompleted(() -> registry.gauge(rootId.withTag("status", "success"), 1))
                .doOnUnsubscribe(() -> unsubscribed.set(1));
    }

    private long getTimeSinceLastEmit(long value) {
        return unsubscribed.get() == 0 ? registry.clock().wallTime() - lastEmitTimestamp.get() : 0;
    }
}
