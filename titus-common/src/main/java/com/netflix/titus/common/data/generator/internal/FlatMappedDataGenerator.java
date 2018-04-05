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

/**
 */
public class FlatMappedDataGenerator<A, B> extends DataGenerator<B> {

    private final DataGenerator<A> source;
    private final DataGenerator<B> pending;
    private final Function<A, DataGenerator<B>> transformer;

    private FlatMappedDataGenerator(DataGenerator<A> source, DataGenerator<B> pending, Function<A, DataGenerator<B>> transformer) {
        this.source = source;
        this.pending = pending;
        this.transformer = transformer;
    }

    @Override
    public DataGenerator<B> apply() {
        DataGenerator<B> next = pending.apply();
        if (!next.isClosed()) {
            return new FlatMappedDataGenerator<>(source, next, transformer);
        }
        return newInstance(source.apply(), transformer);
    }

    @Override
    public Optional<B> getOptionalValue() {
        return pending.getOptionalValue();
    }

    public static <A, B> DataGenerator<B> newInstance(DataGenerator<A> source, Function<A, DataGenerator<B>> transformer) {
        for (DataGenerator<A> nextSource = source; !nextSource.isClosed(); nextSource = nextSource.apply()) {
            DataGenerator<B> nextPending = transformer.apply(nextSource.getValue());
            if (!nextPending.isClosed()) {
                return new FlatMappedDataGenerator<>(nextSource, nextPending, transformer);
            }
        }
        return (DataGenerator<B>) EOS;
    }
}
