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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

class ReactorReEmitterOperator<T> implements Function<Flux<T>, Publisher<T>> {

    private final Function<T, T> transformer;
    private final Duration interval;
    private final Scheduler scheduler;

    ReactorReEmitterOperator(Function<T, T> transformer, Duration interval, Scheduler scheduler) {
        this.transformer = transformer;
        this.interval = interval;
        this.scheduler = scheduler;
    }

    @Override
    public Publisher<T> apply(Flux<T> source) {
        return Flux.create(emitter -> {
            AtomicReference<T> lastItemRef = new AtomicReference<>();
            AtomicReference<Long> reemmitDeadlineRef = new AtomicReference<>();

            Flux<T> reemiter = Flux.interval(interval, interval, scheduler)
                    .flatMap(tick -> {
                        if (lastItemRef.get() != null && reemmitDeadlineRef.get() <= scheduler.now(TimeUnit.MILLISECONDS)) {
                            return Flux.just(transformer.apply(lastItemRef.get()));
                        }
                        return Flux.empty();
                    });

            Disposable disposable = Flux.merge(
                    source.doOnNext(next -> {
                        lastItemRef.set(next);
                        reemmitDeadlineRef.set(scheduler.now(TimeUnit.MILLISECONDS) + interval.toMillis());
                    }),
                    reemiter
            ).subscribe(emitter::next, emitter::error, emitter::complete);

            emitter.onCancel(disposable::dispose);
        });
    }
}
