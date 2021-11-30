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

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public final class FunctionExt {
    private static final Predicate TRUE_PREDICATE = ignored -> true;
    private static final Predicate FALSE_PREDICATE = ignored -> false;
    private static final Runnable NOOP = () -> {
    };

    public static <T> Predicate<T> alwaysTrue() {
        return TRUE_PREDICATE;
    }

    public static <T> Predicate<T> alwaysFalse() {
        return FALSE_PREDICATE;
    }

    public static Runnable noop() {
        return NOOP;
    }

    public static <T> Optional<T> ifNotPresent(Optional<T> opt, Runnable what) {
        if (!opt.isPresent()) {
            what.run();
        }
        return opt;
    }

    /**
     * {@link UnaryOperator} does not have an equivalent of {@link Function#andThen(Function)}, so this can be used when
     * the more specific type ({@link UnaryOperator}) is required.
     */
    public static <T> UnaryOperator<T> andThen(UnaryOperator<T> op, UnaryOperator<T> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(op.apply(t));
    }

}
