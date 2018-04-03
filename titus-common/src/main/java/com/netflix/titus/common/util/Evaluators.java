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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A collection of higher order functions helping in conditional expression evaluations.
 */
public final class Evaluators {

    private Evaluators() {
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

    public static void times(int count, Runnable task) {
        for (int i = 0; i < count; i++) {
            task.run();
        }
    }
}
