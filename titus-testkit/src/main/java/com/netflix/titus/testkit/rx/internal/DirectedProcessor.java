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

package com.netflix.titus.testkit.rx.internal;

import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxProcessor;

public class DirectedProcessor<T> extends FluxProcessor<T, T> {

    private final Supplier<FluxProcessor<T, T>> processorSupplier;

    private volatile FluxProcessor<T, T> activeProcessor;

    public DirectedProcessor(Supplier<FluxProcessor<T, T>> processorSupplier) {
        this.processorSupplier = processorSupplier;
        this.activeProcessor = processorSupplier.get();
    }

    @Override
    public void onSubscribe(Subscription s) {
    }

    @Override
    public void onNext(T t) {
        activeProcessor.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        activeProcessor.onError(t);
    }

    @Override
    public void onComplete() {
        activeProcessor.onComplete();
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> subscriber) {
        activeProcessor
                .doFinally(signalType -> activeProcessor = processorSupplier.get())
                .subscribe(subscriber);
    }
}
