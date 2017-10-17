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

import com.google.common.base.Preconditions;
import io.netflix.titus.common.data.generator.DataGenerator;

public class LimitDataGenerator<A> extends DataGenerator<A> {

    private final DataGenerator<A> source;
    private final int remaining;

    private LimitDataGenerator(DataGenerator<A> source, int count) {
        this.source = source;
        this.remaining = count - 1;
    }

    @Override
    public DataGenerator<A> apply() {
        if (remaining == 0) {
            return (DataGenerator<A>) EOS;
        }
        DataGenerator nextGen = source.apply();
        if (nextGen.isClosed()) {
            return (DataGenerator<A>) EOS;
        }
        return new LimitDataGenerator<>(nextGen, remaining);
    }

    @Override
    public Optional<A> getOptionalValue() {
        return source.getOptionalValue();
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> source, int count) {
        Preconditions.checkArgument(count > 0, "Limit must be > 0");
        if (source.isClosed()) {
            return (DataGenerator<A>) EOS;
        }
        return new LimitDataGenerator<>(source, count);
    }
}
