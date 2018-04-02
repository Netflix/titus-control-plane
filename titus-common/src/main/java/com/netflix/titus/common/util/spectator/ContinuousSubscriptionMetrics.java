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
import rx.Observable;
import rx.Single;

/**
 * This produces a <tt>Transformer</tt> that instruments a {@link Completable}, {@link Single}, or {@link Observable}
 * with metrics to determine its latency as well as how long it has been since it last completed. This is useful when it
 * gets continuously scheduled but has the following limitations:
 * <ul>
 * <li>This class only measures the latency based on the first subscription until the first complete.</li>
 * <li>The same instance of this transformer must be used in order to keep the complete timestamp history.</li>
 * </ul>
 */
public class ContinuousSubscriptionMetrics {
    private final Registry registry;
    private final Timer latency;
    private final AtomicBoolean shouldRemove;
    private final AtomicBoolean hasSubscribed;
    private final AtomicLong latencyStart;
    private final AtomicLong lastCompleteTimestamp;
    private final Id timeSinceLastCompleteId;
    private CompletableTransformer asCompletable;

    ContinuousSubscriptionMetrics(String root, List<Tag> commonTags, Registry registry) {
        this.registry = registry;
        this.latency = registry.timer(root + ".latency", commonTags);
        this.shouldRemove = new AtomicBoolean(false);
        this.hasSubscribed = new AtomicBoolean(false);
        this.latencyStart = new AtomicLong(0);
        this.lastCompleteTimestamp = new AtomicLong(registry.clock().wallTime());
        this.timeSinceLastCompleteId = registry.createId(root + ".timeSinceLastComplete", commonTags);

        PolledMeter.using(registry)
                .withId(timeSinceLastCompleteId)
                .monitorValue(this, ContinuousSubscriptionMetrics::getTimeSinceLastComplete);

        asCompletable = new CompletableTransformer();
    }

    public void remove() {
        shouldRemove.set(true);
        PolledMeter.remove(registry, timeSinceLastCompleteId);
    }

    public Completable.Transformer asCompletable() {
        return asCompletable;
    }

    public <T> Single.Transformer<T, T> asSingle() {
        return new SingleTransformer<T>();
    }

    public <T> Observable.Transformer<T, T> asObservable() {
        return new ObservableTransformer<T>();
    }

    private void onSubscribe() {
        if (!shouldRemove.get() && !hasSubscribed.get()) {
            hasSubscribed.set(true);
            latencyStart.set(registry.clock().wallTime());
        }
    }

    private void onCompleted() {
        if (!shouldRemove.get()) {
            final long end = registry.clock().wallTime();
            latency.record(end - latencyStart.get(), TimeUnit.MILLISECONDS);
            lastCompleteTimestamp.set(registry.clock().wallTime());
            hasSubscribed.set(false);
        }
    }

    private long getTimeSinceLastComplete() {
        if (shouldRemove.get()) {
            return 0;
        }
        return registry.clock().wallTime() - lastCompleteTimestamp.get();
    }

    private class CompletableTransformer implements Completable.Transformer {
        @Override
        public Completable call(Completable completable) {
            return completable
                    .doOnSubscribe(ignored -> onSubscribe())
                    .doOnCompleted(ContinuousSubscriptionMetrics.this::onCompleted);
        }
    }

    private class SingleTransformer<T> implements Single.Transformer<T, T> {
        @Override
        public Single<T> call(Single<T> single) {
            return single
                    .doOnSubscribe(ContinuousSubscriptionMetrics.this::onSubscribe)
                    .doOnError(ignored -> onCompleted())
                    .doOnSuccess(ignored -> onCompleted());
        }
    }

    private class ObservableTransformer<T> implements Observable.Transformer<T, T> {
        @Override
        public Observable<T> call(Observable<T> completable) {
            return completable
                    .doOnSubscribe(ContinuousSubscriptionMetrics.this::onSubscribe)
                    .doOnCompleted(ContinuousSubscriptionMetrics.this::onCompleted);
        }
    }
}
