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

package com.netflix.titus.common.util.cache;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.netflix.titus.common.util.tuple.Pair;

/**
 * Implementation of a {@link Function} that keeps a cache of its last computed result. Sequential calls to
 * {@link MemoizedFunction#apply(T)} with _the same input_ will not yield multiple computations and will return the
 * cached result.
 *
 * @param <T> input type
 * @param <R> result type
 */
public final class MemoizedFunction<T, R> implements Function<T, R> {
    private final BiFunction<T, Optional<R>, R> computation;
    private final AtomicReference<Pair<T, R>> cachedInputResultPair = new AtomicReference<>();

    /**
     * The <tt>computation</tt> function must be idempotent and side effect free, and will receive an {@link Optional}
     * with the last result value (if present), so it can decide to reuse it instead of computing a new one. It may be
     * called multiple times for a single <tt>input</tt> in case of contention (optimistic concurrency).
     */
    public MemoizedFunction(BiFunction<T, Optional<R>, R> computation) {
        this.computation = computation;
    }

    /**
     * Sequential calls with the same <tt>input</tt> will cause a single computation to be executed.
     * <p>
     * This method is thread-safe (optimistic concurrency), and {@link BiFunction computations} can happen multiple
     * times for the same input in case of contention.
     */
    @Override
    public final R apply(T input) {
        Pair<T, R> lastPair;
        Pair<T, R> newPair;

        do {
            lastPair = cachedInputResultPair.get();
            final Optional<R> optionalLastResult = lastPair == null ? Optional.empty() : Optional.of(lastPair.getRight());
            if (optionalLastResult.isPresent()) {
                T lastInput = lastPair.getLeft();
                if (input.equals(lastInput)) {
                    return optionalLastResult.get(); // cached
                }
            }
            newPair = Pair.of(input, computation.apply(input, optionalLastResult));
        } while (!cachedInputResultPair.compareAndSet(lastPair, newPair));

        return newPair.getRight();
    }
}
