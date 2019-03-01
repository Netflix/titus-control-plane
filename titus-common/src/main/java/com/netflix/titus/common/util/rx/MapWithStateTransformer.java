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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import rx.Observable;
import rx.Subscription;

/**
 * See {@link ObservableExt#mapWithState(Object, BiFunction)}.
 */
class MapWithStateTransformer<T, R, S> implements Observable.Transformer<T, R> {

    private final Supplier<S> zeroSupplier;
    private final BiFunction<T, S, Pair<R, S>> transformer;
    private final Observable<Function<S, Pair<R, S>>> cleanupActions;

    MapWithStateTransformer(Supplier<S> zeroSupplier, BiFunction<T, S, Pair<R, S>> transformer, Observable<Function<S, Pair<R, S>>> cleanupActions) {
        this.zeroSupplier = zeroSupplier;
        this.transformer = transformer;
        this.cleanupActions = cleanupActions;
    }

    @Override
    public Observable<R> call(Observable<T> source) {
        return Observable.unsafeCreate(subscriber -> {
            AtomicReference<S> lastState = new AtomicReference<>(zeroSupplier.get());

            Observable<Either<T, Function<S, Pair<R, S>>>> sourceEither = source.map(Either::ofValue);
            Observable<Either<T, Function<S, Pair<R, S>>>> cleanupEither = cleanupActions.map(Either::ofError);
            Subscription subscription = Observable.merge(sourceEither, cleanupEither).subscribe(
                    next -> {
                        Pair<R, S> result;
                        if (next.hasValue()) {
                            try {
                                result = transformer.apply(next.getValue(), lastState.get());
                            } catch (Throwable e) {
                                subscriber.onError(e);
                                return;
                            }
                        } else {
                            try {
                                Function<S, Pair<R, S>> action = next.getError();
                                result = action.apply(lastState.get());
                            } catch (Throwable e) {
                                subscriber.onError(e);
                                return;
                            }
                        }
                        lastState.set(result.getRight());
                        subscriber.onNext(result.getLeft());
                    },
                    subscriber::onError,
                    subscriber::onCompleted
            );
            subscriber.add(subscription);
        });
    }
}
