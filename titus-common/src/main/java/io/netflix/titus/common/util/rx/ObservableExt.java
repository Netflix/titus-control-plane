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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.netflix.titus.common.util.rx.batch.Batch;
import io.netflix.titus.common.util.rx.batch.Batchable;
import io.netflix.titus.common.util.rx.batch.RateLimitedBatcher;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import rx.BackpressureOverflow;
import rx.Completable;
import rx.Notification;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Supplementary Rx operators.
 */
public class ObservableExt {

    /**
     * Wrap {@link Runnable} into observable.
     */
    public static <Void> Observable<Void> fromRunnable(Runnable runnable) {
        return (Observable<Void>) Observable.fromCallable(() -> {
            runnable.run();
            return null;
        }).ignoreElements();
    }

    /**
     * {@link Observable#fromCallable(Callable)} variant, which emits individual collection values.
     */
    public static <T> Observable<T> fromCallable(Supplier<Collection<T>> supplier) {
        return Observable.fromCallable(supplier::get).flatMapIterable(v -> v);
    }

    /**
     * Invokes the completable supplier on each subscription, and connects a client subscription to it.
     */
    public static Completable fromCallableSupplier(Callable<Completable> completableSupplier) {
        return Observable.fromCallable(completableSupplier).flatMap(Completable::toObservable).toCompletable();
    }

    /**
     * Default RxJava future wrapper is blocking. Here we provide polling version.
     */
    public static <T> Observable<T> toObservable(Future<T> future, Scheduler scheduler) {
        return Observable.unsafeCreate(new FutureOnSubscribe<>(future, scheduler));
    }

    /**
     * Subscribes to one observable at a time. Once the current observable completes, waits for a request amount of time
     * before next subscription.
     */
    public static <T> Observable<T> fromWithDelay(List<Observable<T>> chunks, long delay, TimeUnit timeUnit, Scheduler scheduler) {
        if (chunks.isEmpty()) {
            return Observable.empty();
        }
        if (chunks.size() == 1) {
            return chunks.get(0);
        }
        return chunks.get(0)
                .concatWith((Observable<T>) Observable.timer(delay, timeUnit, scheduler).ignoreElements())
                .concatWith(fromWithDelay(chunks.subList(1, chunks.size()), delay, timeUnit, scheduler));
    }

    /**
     * Emit collection content on subscribe.
     */
    public static <T> Observable<T> fromCollection(Supplier<Collection<T>> listProvider) {
        return Observable.fromCallable(listProvider::get).flatMap(Observable::from);
    }

    /**
     * An operator that combines snapshots state with hot updates. To prevent loss of
     * any update for a given snapshot, the hot subscriber is subscribed first, and its
     * values are buffered until the snapshot state is streamed to the subscriber.
     */
    public static <T> Observable.Transformer<T, T> head(Supplier<Collection<T>> headSupplier) {
        return new HeadTransformer<>(headSupplier);
    }

    /**
     * An operator that on subscription creates a user supplied state object, that is combined with each emitted item.
     */
    public static <T, S> Observable.Transformer<T, Pair<T, S>> combine(Supplier<S> stateFactory) {
        return new CombineTransformer<>(stateFactory);
    }

    /**
     * Equivalent to {@link Observable#map} function, but with additional state passing. Each function invocation
     * returns a pair, where the first value is a map result, and the second value is state object, passed as an input
     * when next item is emitted.
     */
    public static <T, R, S> Observable.Transformer<T, R> mapWithState(S zero, BiFunction<T, S, Pair<R, S>> transformer) {
        return new MapWithStateTransformer<>(zero, transformer, Observable.empty());
    }

    /**
     * A variant of {@link #mapWithState(Object, BiFunction)} operator, with a source of cleanup actions.
     */
    public static <T, R, S> Observable.Transformer<T, R> mapWithState(S zero,
                                                                      BiFunction<T, S, Pair<R, S>> transformer,
                                                                      Observable<Function<S, Pair<R, S>>> cleanupActions) {
        return new MapWithStateTransformer<>(zero, transformer, cleanupActions);
    }

    /**
     * Emit single {@link Either} value containing all source observable values, or an error which caused the
     * stream to terminate.
     */
    public static <T> Single<Either<List<T>, Throwable>> toEither(Observable<T> source) {
        return source.toList().toSingle().map(Either::<List<T>, Throwable>ofValue).onErrorReturn(Either::ofError);
    }

    /**
     * Ignore all elements, and emit empty {@link Optional} if stream completes normally or {@link Optional} with
     * the exception.
     */
    public static Single<Optional<Throwable>> emitError(Observable<?> source) {
        return source.ignoreElements().materialize().take(1).map(result ->
                result.getKind() == Notification.Kind.OnError
                        ? Optional.of(result.getThrowable())
                        : Optional.<Throwable>empty()
        ).toSingle();
    }

    /**
     * Wrap and instrument a <tt>batcher</tt> so it can be {@link Observable#compose(Observable.Transformer) composed}
     * into a RxJava chain.
     */
    public static <T extends Batchable<?>, I>
    Observable.Transformer<T, Batch<T, I>> batchWithRateLimit(RateLimitedBatcher<T, I> batcher,
                                                              String metricsRootName,
                                                              Registry registry) {
        return source -> source
                .lift(batcher)
                .compose(SpectatorExt.subscriptionMetrics(metricsRootName, registry));
    }

    /**
     * Wrap and instrument a <tt>batcher</tt> so it can be {@link Observable#compose(Observable.Transformer) composed}
     * into a RxJava chain.
     */
    public static <T extends Batchable<?>, I>
    Observable.Transformer<T, Batch<T, I>> batchWithRateLimit(RateLimitedBatcher<T, I> batcher,
                                                              String metricsRootName,
                                                              List<Tag> tags,
                                                              Registry registry) {
        return source -> source
                .lift(batcher)
                .compose(SpectatorExt.subscriptionMetrics(metricsRootName, tags, registry));
    }

    /**
     * Ignore all elements, and emit empty {@link Optional} if stream completes normally or {@link Optional} with
     * the exception.
     */
    public static Single<Optional<Throwable>> emitError(Completable source) {
        return emitError(source.toObservable());
    }

    /**
     * Creates an observable where on subscription first a context is computed, which is used to create a target observable.
     */
    public static <CONTEXT, T> Observable<T> inContext(Supplier<CONTEXT> contextProvider, Function<CONTEXT, Observable<T>> factory) {
        return Observable.unsafeCreate(new InContextOnSubscribe<>(contextProvider, factory));
    }

    /**
     * Back-pressure enabled infinite stream of data generator.
     */
    public static <T> Observable<T> generatorFrom(Supplier<T> source) {
        return ValueGenerator.from(source, Schedulers.computation());
    }

    /**
     * Back-pressure enabled infinite stream of data generator.
     */
    public static <T> Observable<T> generatorFrom(Function<Long, T> source) {
        return ValueGenerator.from(source, Schedulers.computation());
    }

    /**
     * Back-pressure enabled infinite stream of data generator.
     */
    public static <T> Observable<T> generatorFrom(Supplier<T> source, Scheduler scheduler) {
        return ValueGenerator.from(source, scheduler);
    }

    /**
     * Back-pressure enabled infinite stream of data generator.
     */
    public static <T> Observable<T> generatorFrom(Function<Long, T> source, Scheduler scheduler) {
        return ValueGenerator.from(source, scheduler);
    }

    /**
     * Periodically subscribes to the source observable, and emits all its values (as list) or an exception if
     * the subscription terminates with an error.
     *
     * @param sourceObservable an observable which is periodically subscribed to
     * @param initialDelay     initial delay before the first subscription
     * @param interval         delay time between the last subscription termination, and start of a new one
     * @param timeUnit         time unit for initialDelay and interval parameters
     * @param scheduler        scheduler on which subscription is executed.
     */
    public static <T> Observable<List<T>> periodicGenerator(Observable<T> sourceObservable,
                                                            long initialDelay,
                                                            long interval,
                                                            TimeUnit timeUnit,
                                                            Scheduler scheduler) {
        return PeriodicGenerator.from(sourceObservable, initialDelay, interval, timeUnit, scheduler);
    }

    /**
     * Simple scheduler.
     */
    public static Observable<Optional<Throwable>> schedule(String metricNameRoot,
                                                           Registry registry,
                                                           String completableName,
                                                           Completable completable,
                                                           long initialDelay,
                                                           long interval,
                                                           TimeUnit timeUnit,
                                                           Scheduler scheduler) {
        InstrumentedCompletableScheduler completableScheduler = new InstrumentedCompletableScheduler(metricNameRoot, registry, scheduler);
        return completableScheduler.schedule(completableName, completable, initialDelay, interval, timeUnit);
    }

    /**
     * In case subscriber does not provide exception handler, the error is propagated back to source, and the stream is broken.
     * In some scenarios it is undesirable behavior. This method wraps the unprotected observable, and logs all unhandled
     * exceptions using the provided logger.
     */
    public static <T> Observable<T> protectFromMissingExceptionHandlers(Observable<T> unprotectedStream, Logger logger) {
        return Observable.unsafeCreate(subscriber -> {
            Subscription subscription = unprotectedStream.subscribe(
                    event -> {
                        try {
                            subscriber.onNext(event);
                        } catch (Exception e) {
                            try {
                                subscriber.onError(e);
                            } catch (Exception e2) {
                                logger.warn("Subscriber threw an exception from onNext handler", e);
                                logger.warn("Subscriber threw an exception from onError handler", e2);
                            }
                        }
                    },
                    e -> {
                        try {
                            subscriber.onError(e);
                        } catch (Exception e2) {
                            logger.warn("Subscriber threw an exception from onError handler", e2);
                        }
                    },
                    () -> {
                        try {
                            subscriber.onCompleted();
                        } catch (Exception e) {
                            logger.warn("Subscriber threw an exception from onCompleted handler", e);
                        }
                    }
            );
            subscriber.add(subscription);
        });
    }

    /**
     * Adds a backpressure handler to an observable stream, which buffers data, and in case of buffer overflow
     * calls periodically the user provided callback handler.
     */
    public static <T> Observable<T> onBackpressureDropAndNotify(Observable<T> unprotectedStream,
                                                                long maxBufferSize,
                                                                Consumer<Long> onDropAction,
                                                                long notificationInterval,
                                                                TimeUnit timeUnit) {
        long notificationIntervalMs = timeUnit.toMillis(notificationInterval);

        return Observable.fromCallable(() -> 1).flatMap(tick -> {
            AtomicLong lastOverflowLogTime = new AtomicLong();
            AtomicLong dropCount = new AtomicLong();
            return unprotectedStream.onBackpressureBuffer(
                    maxBufferSize,
                    () -> {
                        dropCount.getAndIncrement();
                        long now = System.currentTimeMillis();
                        if (now - lastOverflowLogTime.get() > notificationIntervalMs) {
                            onDropAction.accept(dropCount.get());
                            lastOverflowLogTime.set(now);
                        }
                    },
                    BackpressureOverflow.ON_OVERFLOW_DROP_OLDEST
            );
        });
    }

    /**
     * Unsubscribe, ignoring null subscription values.
     */
    public static void safeUnsubscribe(Subscription... subscriptions) {
        for (Subscription subscription : subscriptions) {
            if (subscription != null) {
                subscription.unsubscribe();
            }
        }
    }

    /**
     * Subscriber that swallows all {@link Observable} notifications. {@link Observable#subscribe()} throws an
     * exception back to the caller if the subscriber does not provide {@link Subscriber#onError(Throwable)} method
     * implementation.
     */
    public static <T> Subscriber<T> silentSubscriber() {
        return new Subscriber<T>() {
            @Override
            public void onNext(T t) {
                // Do nothing
            }

            @Override
            public void onCompleted() {
                // Do nothing
            }

            @Override
            public void onError(Throwable e) {
                // Do nothing
            }
        };
    }

    /**
     * Transform an optional observable into an observable that emits only present values
     */
    public static <T> Observable<T> fromOptionalObservable(Observable<Optional<T>> observable) {
        return observable.filter(Optional::isPresent).map(Optional::get);
    }

    /**
     * Creates an instrumented event loop
     *
     * @param metricNameRoot the root metric name to use
     * @param registry       the metric registry to use
     * @param scheduler      the scheduler used to create workers
     */
    public static InstrumentedEventLoop createEventLoop(String metricNameRoot, Registry registry, Scheduler scheduler) {
        return new InstrumentedEventLoop(metricNameRoot, registry, scheduler);
    }
}
