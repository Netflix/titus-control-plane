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

package io.netflix.titus.common.util.rx;

import java.util.function.Function;

import rx.Emitter;
import rx.Subscription;
import rx.functions.Cancellable;

public class DecoratedEmitter<T> implements Emitter<T> {
    private final Emitter<T> actual;
    private final Function<T, T> onNextDecorator;

    public DecoratedEmitter(Emitter<T> actual, Function<T, T> onNextDecorator) {
        this.actual = actual;
        this.onNextDecorator = onNextDecorator;
    }

    @Override
    public void setSubscription(Subscription s) {
        actual.setSubscription(s);
    }

    @Override
    public void setCancellation(Cancellable c) {
        actual.setCancellation(c);
    }

    @Override
    public long requested() {
        return actual.requested();
    }

    @Override
    public void onCompleted() {
        actual.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        actual.onError(e);
    }

    @Override
    public void onNext(T t) {
        T decorated = onNextDecorator.apply(t);
        actual.onNext(decorated);
    }
}
