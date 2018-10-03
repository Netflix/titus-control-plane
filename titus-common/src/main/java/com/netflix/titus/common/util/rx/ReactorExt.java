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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import hu.akarnokd.rxjava.interop.RxJavaInterop;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import rx.Observable;
import rx.Single;

/**
 * Supplementary Spring Reactor operators.
 */
public final class ReactorExt {

    private ReactorExt() {
    }

    public static <T> Flux<T> fromListener(Consumer<Consumer<T>> register, Consumer<Consumer<T>> unregister) {
        return null;
    }

    /**
     * If the source observable does not emit any item in the configured amount of time, the last emitted value is
     * re-emitted again, optionally updated by the transformer.
     */
    public static <T> Function<Flux<T>, Publisher<T>> reEmitter(Function<T, T> transformer, long interval, TimeUnit timeUnit, Scheduler scheduler) {
        return new ReactorReEmitterOperator<>(transformer, interval, timeUnit, scheduler);
    }

    /**
     * RxJava {@link Observable} to {@link Flux} bridge.
     */
    public static <T> Flux<T> toFlux(Observable<T> observable) {
        return Flux.create(new FluxObservableEmitter<>(observable));
    }

    /**
     * RxJava {@link Single} to {@link Mono} bridge.
     */
    public static <T> Mono<T> toMono(Single<T> single) {
        return toFlux(single.toObservable()).next();
    }

    /**
     * {@link Mono} bridge to RxJava {@link Observable}.
     */
    public static <T> Observable<T> toObservable(Mono<T> mono) {
        return RxJavaInterop.toV1Observable(mono);
    }

    /**
     * {@link Mono} bridge to RxJava {@link Single}.
     */
    public static <T> Single<T> toSingle(Mono<T> mono) {
        return toObservable(mono).toSingle();
    }
}
