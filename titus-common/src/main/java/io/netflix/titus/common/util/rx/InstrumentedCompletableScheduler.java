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

package io.netflix.titus.common.util.rx;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.common.util.spectator.ContinuousSubscriptionMetrics;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;

public class InstrumentedCompletableScheduler {
    private final String metricNameRoot;
    private final Registry registry;
    private final Scheduler.Worker worker;
    private final Map<String, ContinuousSubscriptionMetrics> completableMetricsByName;

    InstrumentedCompletableScheduler(String metricNameRoot, Registry registry, Scheduler scheduler) {
        this.metricNameRoot = metricNameRoot;
        this.registry = registry;
        this.worker = scheduler.createWorker();
        completableMetricsByName = new ConcurrentHashMap<>();
    }

    public Observable<Optional<Throwable>> schedule(String completableName, Completable completable, long initialDelay,
                                                    long interval, TimeUnit timeUnit) {
        return Observable.create(emitter -> {
            ContinuousSubscriptionMetrics subscriptionMetrics = this.completableMetricsByName.computeIfAbsent(completableName, k -> {
                String rootName = metricNameRoot + ".completableScheduler." + completableName;
                return SpectatorExt.continuousSubscriptionMetrics(rootName, Collections.emptyList(), registry);
            });
            Action0 action = () -> ObservableExt.emitError(completable.compose(subscriptionMetrics.asCompletable()))
                    .subscribe(emitter::onNext, emitter::onError);
            Subscription subscription = worker.schedulePeriodically(action, initialDelay, interval, timeUnit);
            emitter.setCancellation(() -> {
                subscription.unsubscribe();
                subscriptionMetrics.remove();
                completableMetricsByName.remove(completableName);
            });
        }, Emitter.BackpressureMode.NONE);
    }

    public void shutdown() {
        worker.unsubscribe();
        completableMetricsByName.values().forEach(ContinuousSubscriptionMetrics::remove);
        completableMetricsByName.clear();
    }
}
