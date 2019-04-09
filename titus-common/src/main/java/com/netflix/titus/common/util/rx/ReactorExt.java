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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.netflix.titus.common.util.tuple.Pair;
import hu.akarnokd.rxjava.interop.RxJavaInterop;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import reactor.core.CoreSubscriber;
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
     * Ignore all elements, and emit empty {@link Optional#empty()} if stream completes normally or
     * {@link Optional#of(Object)} with the exception.
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
     * An operator that converts collection of value into an event stream. Subsequent collection versions
     * are compared against each other and add/remove events are emitted accordingly.
     */
    public static <K, T, E> Function<Flux<List<T>>, Publisher<E>> eventEmitter(
            Function<T, K> keyFun,
            BiPredicate<T, T> valueComparator,
            Function<T, E> valueAddedEventMapper,
            Function<T, E> valueRemovedEventMapper,
            E snapshotEndEvent) {
        return new EventEmitterTransformer<>(keyFun, valueComparator, valueAddedEventMapper, valueRemovedEventMapper, snapshotEndEvent);
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
     * Equivalent to {@link Observable#map} function, but with additional state passing. Each function invocation
     * returns a pair, where the first value is a map result, and the second value is state object, passed as an input
     * when next item is emitted.
     */
    public static <T, R, S> Function<Flux<T>, Publisher<R>> mapWithState(S zero, BiFunction<T, S, Pair<R, S>> transformer) {
        return new ReactorMapWithStateTransformer<>(() -> zero, transformer, Flux.empty());
    }

    /**
     * See {@link #mapWithState(Object, BiFunction)}. The difference is that the initial value is computed on each subscription.
     */
    public static <T, R, S> Function<Flux<T>, Publisher<R>> mapWithState(Supplier<S> zeroSupplier, BiFunction<T, S, Pair<R, S>> transformer) {
        return new ReactorMapWithStateTransformer<>(zeroSupplier, transformer, Flux.empty());
    }

    /**
     * A variant of {@link #mapWithState(Object, BiFunction)} operator, with a source of cleanup actions.
     */
    public static <T, R, S> Function<Flux<T>, Publisher<R>> mapWithState(S zero,
                                                                         BiFunction<T, S, Pair<R, S>> transformer,
                                                                         Flux<Function<S, Pair<R, S>>> cleanupActions) {
        return new ReactorMapWithStateTransformer<>(() -> zero, transformer, cleanupActions);
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
                                logger.warn("Subscriber threw an exception from onNext handler", e);
                            } catch (Exception e2) {
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
     * RxJava {@link Single} to {@link Mono} bridge.
     */
    public static Mono<Void> toMono(Observable<Void> observable) {
        return toFlux(observable).next();
    }

    /**
     * RxJava {@link rx.Completable} to {@link Mono} bridge.
     */
    public static Mono<Void> toMono(Completable completable) {
        return toFlux(completable.<Void>toObservable()).next();
    }

    /**
     * {@link Flux} bridge to RxJava {@link Observable}.
     */
    public static <T> Observable<T> toObservable(Flux<T> flux) {
        return RxJavaInterop.toV1Observable(flux);
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

    /**
     * {@link Mono} bridge to RxJava {@link Completable}.
     */
    public static Completable toCompletable(Mono<Void> mono) {
        return toObservable(mono).toCompletable();
    }

    /**
     * Subscriber that does not do anything with the callback requests.
     */
    public static <T> CoreSubscriber<T> silentSubscriber() {
        return new CoreSubscriber<T>() {
            @Override
            public void onSubscribe(Subscription s) {
            }

            @Override
            public void onNext(T t) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        };
    }
}
