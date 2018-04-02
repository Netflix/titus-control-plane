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

package com.netflix.titus.common.data.generator.internal;

import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;

public class FromContextGenerator<CONTEXT, V> extends DataGenerator<V> {

    private final Function<CONTEXT, Pair<CONTEXT, Optional<V>>> valueSupplier;
    private final Pair<CONTEXT, Optional<V>> current;

    private FromContextGenerator(CONTEXT initial, Function<CONTEXT, Pair<CONTEXT, Optional<V>>> valueSupplier) {
        this.valueSupplier = valueSupplier;
        this.current = valueSupplier.apply(initial);
    }

    @Override
    public DataGenerator<V> apply() {
        if (current.getRight().isPresent()) {
            return newInstance(current.getLeft(), valueSupplier);
        }
        return (DataGenerator<V>) EOS;
    }

    @Override
    public Optional<V> getOptionalValue() {
        return current.getRight();
    }

    public static <V, CONTEXT> DataGenerator<V> newInstance(CONTEXT initial, Function<CONTEXT, Pair<CONTEXT, Optional<V>>> valueSupplier) {
        return new FromContextGenerator<>(initial, valueSupplier);
    }
}
