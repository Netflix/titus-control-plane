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

public class LoopedDataGenerator<A> extends DataGenerator<A> {

    private final DataGenerator<A> reference;
    private final DataGenerator<A> currentSource;
    private final int iterationNumber;
    private final BiFunction<A, Integer, A> decorator;
    private final Optional<A> current;

    private LoopedDataGenerator(DataGenerator<A> reference,
                                DataGenerator<A> currentSource,
                                int iterationNumber,
                                BiFunction<A, Integer, A> decorator) {
        this.reference = reference;
        this.currentSource = currentSource;
        this.iterationNumber = iterationNumber;
        this.decorator = decorator;
        this.current = Optional.of(decorator.apply(currentSource.getValue(), iterationNumber));
    }

    @Override
    public DataGenerator<A> apply() {
        DataGenerator<A> nextGen = currentSource.apply();
        if (nextGen.isClosed()) {
            return new LoopedDataGenerator<>(reference, reference, iterationNumber + 1, decorator);
        }
        return new LoopedDataGenerator<>(reference, nextGen, iterationNumber, decorator);
    }

    @Override
    public Optional<A> getOptionalValue() {
        return current;
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> source) {
        return newInstance(source, (a, idx) -> a);
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> source, BiFunction<A, Integer, A> decorator) {
        if (source.isClosed()) {
            return (DataGenerator<A>) EOS;
        }
        return new LoopedDataGenerator<>(source, source, 0, decorator);
    }
}
