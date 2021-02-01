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

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

public class ReactorRetriers {

    public static Retry instrumentedReactorRetryer(String name, Duration retryInterval, Logger logger) {
        return new Retry() {
            @Override
            public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals) {
                return retrySignals.flatMap(rs -> {
                    logger.warn("Retrying subscription for {} after {}ms due to an error: error={}", name, retryInterval.toMillis(), rs.failure().getMessage());
                    return Flux.interval(retryInterval).take(1);
                });
            }
        };
    }

    public static <T> Function<Flux<T>, Publisher<T>> instrumentedRetryer(String name, Duration retryInterval, Logger logger) {
        return source -> source.retryWhen(instrumentedReactorRetryer(name, retryInterval, logger));
    }

    public static Retry reactorRetryer(Function<? super Throwable, ? extends Publisher<?>> fn) {
        return new Retry() {
            @Override
            public Publisher<?> generateCompanion(Flux<RetrySignal> retrySignals) {
                return retrySignals
                        .map(RetrySignal::failure)
                        .flatMap(fn);
            }
        };
    }

    public static Retry rectorPredicateRetryer(Predicate<Throwable> predicate) {
        return Retry.from(companion -> companion.handle((retrySignal, sink) -> {
            if (retrySignal.failure() != null && predicate.test(retrySignal.failure())) {
                sink.next(1);
            } else {
                sink.error(retrySignal.failure());
            }
        }));
    }
}
