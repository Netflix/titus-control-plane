/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.network.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.network.client.internal.WebClientMetric;
import com.netflix.titus.common.util.time.Clock;
import org.springframework.http.HttpMethod;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;

// TODO: same operator for Flux when needed
class LatencyRecorder<T> extends MonoOperator<T, T> {
    private final Clock clock;
    private final WebClientMetric metrics;
    private final HttpMethod method;
    private final Optional<String> path;

    LatencyRecorder(Clock clock, Mono<T> source, WebClientMetric metrics, HttpMethod method) {
        super(source);
        this.clock = clock;
        this.metrics = metrics;
        this.method = method;
        this.path = Optional.empty();
    }

    LatencyRecorder(Clock clock, Mono<T> source, WebClientMetric metrics, HttpMethod method, String path) {
        super(source);
        this.clock = clock;
        this.metrics = metrics;
        this.method = method;
        this.path = Optional.of(path);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        AtomicLong startTimeMsHolder = new AtomicLong();
        source.subscribe(
                actual::onNext,
                error -> {
                    long startTimeMs = startTimeMsHolder.get();
                    Preconditions.checkState(startTimeMs > 0, "onError() called before onSubscribe()");
                    Duration elapsed = Duration.ofMillis(clock.wallTime() - startTimeMs);
                    if (path.isPresent()) {
                        metrics.registerOnErrorLatency(method, path.get(), elapsed);
                    } else {
                        metrics.registerOnErrorLatency(method, elapsed);
                    }
                    actual.onError(error);
                },
                () -> {
                    long startTimeMs = startTimeMsHolder.get();
                    Preconditions.checkState(startTimeMs > 0, "onComplete() called before onSubscribe()");
                    Duration elapsed = Duration.ofMillis(clock.wallTime() - startTimeMs);
                    if (path.isPresent()) {
                        metrics.registerOnSuccessLatency(method, path.get(), elapsed);
                    } else {
                        metrics.registerOnSuccessLatency(method, elapsed);
                    }
                    actual.onComplete();
                },
                subscription -> {
                    startTimeMsHolder.set(clock.wallTime());
                    actual.onSubscribe(subscription);
                }
        );
    }
}
