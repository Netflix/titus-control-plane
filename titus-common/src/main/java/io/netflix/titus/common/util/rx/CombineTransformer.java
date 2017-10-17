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

import java.util.function.Supplier;

import io.netflix.titus.common.util.tuple.Pair;
import rx.Observable;

/**
 */
class CombineTransformer<T, S> implements Observable.Transformer<T, Pair<T, S>> {

    private final Supplier<S> stateFactory;

    CombineTransformer(Supplier<S> stateFactory) {
        this.stateFactory = stateFactory;
    }

    @Override
    public Observable<Pair<T, S>> call(Observable<T> source) {
        return Observable.unsafeCreate(subscriber -> {
            S state = stateFactory.get();
            source.map(v -> Pair.of(v, state)).subscribe(subscriber);
        });
    }
}
