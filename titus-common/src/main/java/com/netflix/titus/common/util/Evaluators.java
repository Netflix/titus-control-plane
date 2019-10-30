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

package com.netflix.titus.common.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * A collection of higher order functions helping in conditional expression evaluations.
 */
public final class Evaluators {

    private Evaluators() {
    }

    /**
     * Do nothing consumer.
     */
    public static <T> void doNothing(T value) {
    }

    /**
     * Evaluate the given function only if the value is not null.
     */
    public static <T> void acceptNotNull(T value, Consumer<T> consumer) {
        if (value != null) {
            consumer.accept(value);
        }
    }

    /**
     * Evaluate the given consumer only if the value is true.
     */
    public static <T> void acceptIfTrue(T value, Consumer<Boolean> consumer) {
        if (value instanceof Boolean && value == Boolean.TRUE) {
            consumer.accept(true);
        } else if (value instanceof String && Boolean.parseBoolean((String) value)) {
            consumer.accept(true);
        }
    }

    /**
     * Evaluate the given function only if value is not null and return the evaluated value.
     * Otherwise return null.
     */
    public static <T, R> R applyNotNull(T value, Function<T, R> transformer) {
        return value != null ? transformer.apply(value) : null;
    }

    public static <T> T getOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * Returns first non-null value
     */
    public static <T> T getFirstNotNull(T... values) {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public static void times(int count, Runnable task) {
        for (int i = 0; i < count; i++) {
            task.run();
        }
    }

    public static void times(int count, Consumer<Integer> task) {
        for (int i = 0; i < count; i++) {
            task.accept(i);
        }
    }

    public static <T> List<T> evaluateTimes(int count, Function<Integer, T> transformer) {
        Preconditions.checkArgument(count >= 0);
        if (count == 0) {
            return Collections.emptyList();
        }
        List<T> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(transformer.apply(i));
        }
        return result;
    }

    @SafeVarargs
    public static <T> Optional<T> firstPresent(Supplier<Optional<T>>... optionalSuppliers) {
        for (Supplier<Optional<T>> optionalSupplier : optionalSuppliers) {
            Optional<T> optional = optionalSupplier.get();
            if (optional.isPresent()) {
                return optional;
            }
        }
        return Optional.empty();
    }

    /**
     * Remember last function execution result, and return the cached value on subsequent invocations, until the
     * argument changes.
     * <h1>
     * For memoizing suppliers use {@link com.google.common.base.Suppliers#memoize(com.google.common.base.Supplier)}.
     * </h1>
     */
    public static <T, R> Function<T, R> memoizeLast(Function<T, R> function) {
        AtomicReference<Pair<T, Either<R, Throwable>>> lastRef = new AtomicReference<>();
        return argument -> {
            Pair<T, Either<R, Throwable>> last = lastRef.get();
            if (last != null && Objects.equals(argument, last.getLeft())) {
                if (last.getRight().hasValue()) {
                    return last.getRight().getValue();
                }
                Throwable error = last.getRight().getError();
                if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                }
                if (error instanceof Error) {
                    throw (Error) error;
                }
                throw new IllegalStateException(error);
            }
            try {
                R result = function.apply(argument);
                lastRef.set(Pair.of(argument, Either.ofValue(result)));
                return result;
            } catch (Throwable e) {
                lastRef.set(Pair.of(argument, Either.ofError(e)));
                throw e;
            }
        };
    }

    /**
     * Implementation of a {@link Function} that keeps a cache of its last computed result. Sequential calls to
     * the memoized function with _the same input_ will not yield multiple computations and will return the
     * cached result.
     */
    public static <T, R> Function<T, R> memoizeLast(BiFunction<T, Optional<R>, R> computation) {
        AtomicReference<Pair<T, R>> cachedInputResultPair = new AtomicReference<>();
        return argument -> {
            Pair<T, R> lastPair = cachedInputResultPair.get();
            final Optional<R> optionalLastResult = lastPair == null ? Optional.empty() : Optional.of(lastPair.getRight());
            if (optionalLastResult.isPresent()) {
                T lastArgument = lastPair.getLeft();
                if (Objects.equals(argument, lastArgument)) {
                    return optionalLastResult.get(); // cached
                }
            }
            Pair<T, R> newPair = Pair.of(argument, computation.apply(argument, optionalLastResult));
            cachedInputResultPair.set(newPair);

            return newPair.getRight();
        };
    }
}
