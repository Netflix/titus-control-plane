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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;
import rx.Subscription;

class MapWithStateTransformer<T, R, S> implements Observable.Transformer<T, R> {

    private final S zero;
    private final BiFunction<T, S, Pair<R, S>> transformer;

    MapWithStateTransformer(S zero, BiFunction<T, S, Pair<R, S>> transformer) {
        this.zero = zero;
        this.transformer = transformer;
    }

    @Override
    public Observable<R> call(Observable<T> source) {
        return Observable.unsafeCreate(subscriber -> {
            AtomicReference<S> lastState = new AtomicReference<>(zero);
            Subscription subscription = source.subscribe(
                    next -> {
                        Pair<R, S> result;
                        try {
                            result = transformer.apply(next, lastState.get());
                        } catch (Throwable e) {
                            subscriber.onError(e);
                            return;
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
