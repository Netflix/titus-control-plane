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

package io.netflix.titus.common.util.tuple;

import java.util.function.Function;

/**
 * A container type that holds either a value or an error. It provides a collection of additional methods
 * similar two {@link java.util.Optional}.
 */
public class Either<T, E> {

    private final T value;
    private final E error;

    private Either(T value, E error) {
        this.value = value;
        this.error = error;
    }

    public T getValue() {
        return value;
    }

    public E getError() {
        return error;
    }

    public boolean hasValue() {
        return value != null;
    }

    public boolean hasError() {
        return !hasValue();
    }

    public <U> Either<U, ? extends E> flatMap(Function<T, Either<U, ? extends E>> mapper) {
        if (hasError()) {
            return ofError(error);
        }
        return mapper.apply(value);
    }

    public <U> Either<U, E> map(Function<T, U> mapper) {
        if (hasError()) {
            return ofError(error);
        }
        return ofValue(mapper.apply(value));
    }

    public T onErrorGet(Function<E, T> fallback) {
        if (hasValue()) {
            return value;
        }
        return fallback.apply(error);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Either<?, ?> either = (Either<?, ?>) o;

        if (value != null ? !value.equals(either.value) : either.value != null) {
            return false;
        }
        return error != null ? error.equals(either.error) : either.error == null;

    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (error != null ? error.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return hasValue()
                ? "Either{value=" + value + '}'
                : "Either{error=" + error + '}';
    }

    public static <T, E> Either<T, E> ofValue(T value) {
        return new Either<>(value, null);
    }

    public static <T, E> Either<T, E> ofError(E error) {
        return new Either<>(null, error);
    }
}
