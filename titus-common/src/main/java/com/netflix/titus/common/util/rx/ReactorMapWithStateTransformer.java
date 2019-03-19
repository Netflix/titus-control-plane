/*
 * Copyright 2019 Netflix, Inc.
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
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * See {@link ReactorExt#mapWithState(Object, BiFunction)}.
 */
class ReactorMapWithStateTransformer<T, R, S> implements Function<Flux<T>, Publisher<R>> {

    private final Supplier<S> zeroSupplier;
    private final BiFunction<T, S, Pair<R, S>> transformer;
    private final Flux<Function<S, Pair<R, S>>> cleanupActions;

    ReactorMapWithStateTransformer(Supplier<S> zeroSupplier, BiFunction<T, S, Pair<R, S>> transformer, Flux<Function<S, Pair<R, S>>> cleanupActions) {
        this.zeroSupplier = zeroSupplier;
        this.transformer = transformer;
        this.cleanupActions = cleanupActions;
    }

    @Override
    public Publisher<R> apply(Flux<T> source) {
        return Flux.create(sink -> {
            AtomicReference<S> lastState = new AtomicReference<>(zeroSupplier.get());

            Flux<Either<T, Function<S, Pair<R, S>>>> sourceEither = source.map(Either::ofValue);
            Flux<Either<T, Function<S, Pair<R, S>>>> cleanupEither = cleanupActions.map(Either::ofError);
            Disposable subscription = Flux.merge(sourceEither, cleanupEither).subscribe(
                    next -> {
                        Pair<R, S> result;
                        if (next.hasValue()) {
                            try {
                                result = transformer.apply(next.getValue(), lastState.get());
                            } catch (Throwable e) {
                                sink.error(e);
                                return;
                            }
                        } else {
                            try {
                                Function<S, Pair<R, S>> action = next.getError();
                                result = action.apply(lastState.get());
                            } catch (Throwable e) {
                                sink.error(e);
                                return;
                            }
                        }
                        lastState.set(result.getRight());
                        sink.next(result.getLeft());
                    },
                    sink::error,
                    sink::complete
            );
            sink.onCancel(subscription);
        });
    }
}
