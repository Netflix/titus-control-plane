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
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import hu.akarnokd.rxjava.interop.RxJavaInterop;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import rx.Completable;
import rx.Observable;
import rx.Single;

/**
 * Supplementary Spring Reactor operators.
 */
public final class ReactorExt {

    private ReactorExt() {
    }

    /**
     * Ignore all elements, and emit empty {@link Optional} if stream completes normally or {@link Optional} with
     * the exception.
     */
    public static Mono<Optional<Throwable>> emitError(Mono<?> source) {
        return source.ignoreElement().materialize().map(result ->
                result.getType() == SignalType.ON_ERROR
                        ? Optional.of(result.getThrowable())
                        : Optional.empty()
        );
    }

    public static <L, T> Flux<T> fromListener(Class<L> listener, Consumer<L> register, Consumer<L> unregister) {
        return FluxListenerInvocationHandler.adapt(listener, register, unregister);
    }

    /**
     * In case subscriber does not provide exception handler, the error is propagated back to source, and the stream is broken.
     * In some scenarios it is undesirable behavior. This method wraps the unprotected observable, and logs all unhandled
     * exceptions using the provided logger.
     */
    public static <T> Function<Flux<T>, Publisher<T>> badSubscriberHandler(Logger logger) {
        return source -> Flux.create(emitter -> {
            Disposable subscription = source.subscribe(
                    event -> {
                        try {
                            emitter.next(event);
                        } catch (Exception e) {
                            try {
                                emitter.error(e);
                            } catch (Exception e2) {
                                logger.warn("Subscriber threw an exception from onNext handler", e);
                                logger.warn("Subscriber threw an exception from onError handler", e2);
                            }
                        }
                    },
                    e -> {
                        try {
                            emitter.error(e);
                        } catch (Exception e2) {
                            logger.warn("Subscriber threw an exception from onError handler", e2);
                        }
                    },
                    () -> {
                        try {
                            emitter.complete();
                        } catch (Exception e) {
                            logger.warn("Subscriber threw an exception from onCompleted handler", e);
                        }
                    }
            );
            emitter.onDispose(() -> safeDispose(subscription));
        });
    }

    /**
     * An operator that combines snapshots state with hot updates. To prevent loss of
     * any update for a given snapshot, the hot subscriber is subscribed first, and its
     * values are buffered until the snapshot state is streamed to the subscriber.
     */
    public static <T> Function<Flux<T>, Publisher<T>> head(Supplier<Collection<T>> headSupplier) {
        return new ReactorHeadTransformer<>(headSupplier);
    }

    /**
     * Merge a map of {@link Mono}s, and return the combined result as a map. If a mono with a given key succeeded, the
     * returned map will contain value {@link Optional#empty()} for that key. Otherwise it will contain an error entry.
     */
    public static <K> Mono<Map<K, Optional<Throwable>>> merge(Map<K, Mono<Void>> monos, int concurrencyLimit, Scheduler scheduler) {
        return ReactorMergeOperations.merge(monos, concurrencyLimit, scheduler);
    }

    /**
     * If the source observable does not emit any item in the configured amount of time, the last emitted value is
     * re-emitted again, optionally updated by the transformer.
     */
    public static <T> Function<Flux<T>, Publisher<T>> reEmitter(Function<T, T> transformer, long interval, TimeUnit timeUnit, Scheduler scheduler) {
        return new ReactorReEmitterOperator<>(transformer, interval, timeUnit, scheduler);
    }

    public static void safeDispose(Disposable... disposables) {
        for (Disposable disposable : disposables) {
            try {
                disposable.dispose();
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Runs an action on the provided worker.
     */
    public static <T> Mono<T> onWorker(Supplier<T> action, Scheduler.Worker worker) {
        return TimerWithWorker.timer(action, worker, Duration.ZERO);
    }

    /**
     * Runs an action on the provided worker with the provided delay.
     */
    public static <T> Mono<T> onWorker(Supplier<T> action, Scheduler.Worker worker, Duration delay) {
        return TimerWithWorker.timer(action, worker, delay);
    }

    /**
     * Runs an action on the provided worker.
     */
    public static Mono<Void> onWorker(Runnable action, Scheduler.Worker worker) {
        return TimerWithWorker.timer(action, worker, Duration.ZERO);
    }

    /**
     * Runs an action on the provided worker with the provided delay.
     */
    public static Mono<Void> onWorker(Runnable action, Scheduler.Worker worker, Duration delay) {
        return TimerWithWorker.timer(action, worker, delay);
    }

    /**
     * In case subscriber does not provide exception handler, the error is propagated back to source, and the stream is broken.
     * In some scenarios it is undesirable behavior. This method wraps the unprotected flux, and logs all unhandled
     * exceptions using the provided logger.
     */
    public static <T> Flux<T> protectFromMissingExceptionHandlers(Flux<T> unprotectedStream, Logger logger) {
        return Flux.create(sink -> {
            Disposable disposable = unprotectedStream.subscribe(
                    event -> {
                        try {
                            sink.next(event);
                        } catch (Exception e) {
                            try {
                                sink.error(e);
                            } catch (Exception e2) {
                                logger.warn("Subscriber threw an exception from onNext handler", e);
                                logger.warn("Subscriber threw an exception from onError handler", e2);
                            }
                        }
                    },
                    e -> {
                        try {
                            sink.error(e);
                        } catch (Exception e2) {
                            logger.warn("Subscriber threw an exception from onError handler", e2);
                        }
                    },
                    () -> {
                        try {
                            sink.complete();
                        } catch (Exception e) {
                            logger.warn("Subscriber threw an exception from onCompleted handler", e);
                        }
                    }
            );
            sink.onDispose(disposable);
        });
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
     * RxJava {@link rx.Completable} to {@link Mono} bridge.
     */
    public static Mono<Void> toMono(Completable completable) {
        return toFlux(completable.<Void>toObservable()).next();
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
