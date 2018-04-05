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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;

public class BatchDataGenerator<T> extends DataGenerator<List<T>> {

    private final DataGenerator<T> source;
    private final int batchSize;
    private final Optional<List<T>> currentBatch;

    private BatchDataGenerator(DataGenerator<T> source, int batchSize) {
        this.batchSize = batchSize;
        Pair<List<T>, DataGenerator<T>> pair = nextBatch(source, batchSize);
        this.currentBatch = Optional.of(pair.getLeft());
        this.source = pair.getRight();
    }

    @Override
    public DataGenerator<List<T>> apply() {
        if (source.isClosed()) {
            return (DataGenerator<List<T>>) EOS;
        }
        return new BatchDataGenerator<>(source, batchSize);
    }

    @Override
    public Optional<List<T>> getOptionalValue() {
        return currentBatch;
    }

    private Pair<List<T>, DataGenerator<T>> nextBatch(DataGenerator<T> source, int batchSize) {
        List<T> batch = new ArrayList<>(batchSize);
        DataGenerator<T> nextGen = source;
        for (int i = 0; i < batchSize && !nextGen.isClosed(); i++) {
            batch.add(nextGen.getValue());
            nextGen = nextGen.apply();
        }
        return Pair.of(batch, nextGen);
    }

    public static <A> DataGenerator<List<A>> newInstance(DataGenerator<A> source, int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        if (source.isClosed()) {
            return (DataGenerator<List<A>>) EOS;
        }
        return new BatchDataGenerator<>(source, batchSize);
    }
}
