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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Notification;
import rx.Observable;
import rx.Scheduler;
import rx.Single;
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
        return new MapWithStateTransformer(zero, transformer);
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
    public static Observable<Optional<Throwable>> schedule(Completable action, long initialDelay, long interval, TimeUnit timeUnit, Scheduler scheduler) {
        return Observable.timer(initialDelay, timeUnit, scheduler)
                .flatMap(tick -> emitError(action).toObservable())
                .flatMap(oneRoundResult -> {
                    Observable<Optional<Throwable>> nextRound = schedule(action, interval, interval, timeUnit, scheduler);
                    return Observable.just(oneRoundResult).concatWith(nextRound);
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
     * Transform an optional observable into an observable that emits only present values
     */
    public static <T> Observable<T> fromOptionalObservable(Observable<Optional<T>> observable) {
        return observable.filter(Optional::isPresent).map(Optional::get);
    }
}
