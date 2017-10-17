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

import io.netflix.titus.common.data.generator.DataGenerator;

public class ConcatDataGenerator<A> extends DataGenerator<A> {

    private final DataGenerator<A> first;
    private final DataGenerator<A> second;

    private ConcatDataGenerator(DataGenerator<A> first, DataGenerator<A> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public DataGenerator<A> apply() {
        return newInstance(first.apply(), second);
    }

    @Override
    public Optional<A> getOptionalValue() {
        return first.getOptionalValue();
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> first, DataGenerator<A> second) {
        if (first.isClosed()) {
            if (second.isClosed()) {
                return (DataGenerator<A>) EOS;
            }
            return second;
        }
        if (second.isClosed()) {
            return first;
        }
        return new ConcatDataGenerator<>(first, second);
    }
}
