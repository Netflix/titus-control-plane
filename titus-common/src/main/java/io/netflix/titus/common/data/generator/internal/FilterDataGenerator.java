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

package io.netflix.titus.common.data.generator.internal;

import java.util.Optional;
import java.util.function.Function;

import io.netflix.titus.common.data.generator.DataGenerator;

public class FilterDataGenerator<A> extends DataGenerator<A> {

    private final DataGenerator<A> source;
    private final Function<A, Boolean> predicate;

    private FilterDataGenerator(DataGenerator<A> source, Function<A, Boolean> predicate) {
        this.source = findNext(source, predicate);
        this.predicate = predicate;
    }

    @Override
    public DataGenerator<A> apply() {
        DataGenerator<A> nextGen = source.apply();
        if (nextGen.isClosed()) {
            return (DataGenerator<A>) EOS;
        }
        return new FilterDataGenerator<>(nextGen, predicate);
    }

    @Override
    public Optional<A> getOptionalValue() {
        return source.getOptionalValue();
    }

    private static <A> DataGenerator<A> findNext(DataGenerator<A> source, Function<A, Boolean> predicate) {
        DataGenerator<A> nextGen = source;
        while (!nextGen.isClosed()) {
            if (predicate.apply(nextGen.getValue())) {
                return nextGen;
            }
            nextGen = nextGen.apply();
        }
        return (DataGenerator<A>) EOS;
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> source, Function<A, Boolean> predicate) {
        if (source.isClosed()) {
            return (DataGenerator<A>) EOS;
        }
        return new FilterDataGenerator<>(source, predicate);
    }
}
