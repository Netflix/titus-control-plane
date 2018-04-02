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

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;

public class ZipDataGenerator<A, B> extends DataGenerator<Pair<A, B>> {
    private final DataGenerator<A> first;
    private final DataGenerator<B> second;
    private final Optional<Pair<A, B>> current;

    private ZipDataGenerator(DataGenerator<A> first, DataGenerator<B> second) {
        this.first = first;
        this.second = second;
        this.current = Optional.of(Pair.of(first.getValue(), second.getValue()));
    }

    @Override
    public DataGenerator<Pair<A, B>> apply() {
        return newInstance(first.apply(), second.apply());
    }

    @Override
    public Optional<Pair<A, B>> getOptionalValue() {
        return current;
    }

    public static <A, B> DataGenerator<Pair<A, B>> newInstance(DataGenerator<A> first, DataGenerator<B> second) {
        if (first.isClosed() || second.isClosed()) {
            return (DataGenerator<Pair<A, B>>) EOS;
        }
        return new ZipDataGenerator<>(first, second);
    }
}
