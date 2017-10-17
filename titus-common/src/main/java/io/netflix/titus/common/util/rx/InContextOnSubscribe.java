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

import java.util.function.Function;
import java.util.function.Supplier;

import rx.Observable;
import rx.Subscriber;

class InContextOnSubscribe<CONTEXT, T> implements Observable.OnSubscribe<T> {

    private final Supplier<CONTEXT> contextProvider;
    private final Function<CONTEXT, Observable<T>> factory;

    InContextOnSubscribe(Supplier<CONTEXT> contextProvider,
                         Function<CONTEXT, Observable<T>> factory) {
        this.contextProvider = contextProvider;
        this.factory = factory;
    }

    @Override
    public void call(Subscriber<? super T> subscriber) {
        CONTEXT context;
        try {
            context = contextProvider.get();
        } catch (Exception e) {
            subscriber.onError(e);
            return;
        }

        Observable<T> result;
        try {
            result = factory.apply(context);
        } catch (Exception e) {
            subscriber.onError(e);
            return;
        }
        subscriber.add(result.subscribe(subscriber));
    }
}
