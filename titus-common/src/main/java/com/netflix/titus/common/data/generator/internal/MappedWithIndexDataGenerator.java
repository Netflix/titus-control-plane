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
import java.util.function.BiFunction;

import com.netflix.titus.common.data.generator.DataGenerator;

public class MappedWithIndexDataGenerator<A, B> extends DataGenerator<B> {

    private final DataGenerator<A> sourceGenerator;
    private final BiFunction<Long, A, B> transformer;
    private final long index;
    private final Optional<B> currentValue;

    private MappedWithIndexDataGenerator(DataGenerator<A> sourceGenerator, BiFunction<Long, A, B> transformer, long index) {
        this.sourceGenerator = sourceGenerator;
        this.transformer = transformer;
        this.index = index;
        this.currentValue = sourceGenerator.getOptionalValue().map(t -> transformer.apply(index, t));
    }

    @Override
    public DataGenerator<B> apply() {
        return currentValue.isPresent() ? new MappedWithIndexDataGenerator<>(sourceGenerator.apply(), transformer, index + 1) : (DataGenerator<B>) EOS;
    }

    @Override
    public Optional<B> getOptionalValue() {
        return currentValue;
    }

    public static <A, B> DataGenerator<B> newInstance(DataGenerator<A> sourceGenerator, BiFunction<Long, A, B> transformer) {
        if (sourceGenerator.isClosed()) {
            return (DataGenerator<B>) EOS;
        }
        return new MappedWithIndexDataGenerator<>(sourceGenerator, transformer, 0);
    }
}
