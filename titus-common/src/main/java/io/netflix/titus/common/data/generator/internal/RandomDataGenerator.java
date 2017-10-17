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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.common.util.tuple.Pair;

public class RandomDataGenerator<A> extends DataGenerator<A> {

    private static final double DEFAULT_DENSITY = 0.3;
    private static final int DEFAULT_INITIAL_CHUNK_SIZE = 10;

    private final Random random;
    private final double density;
    private final int slice;

    private final DataGenerator<A> source;
    private final List<A> items;
    private final int position;
    private final int scaleUpLevel;
    private final Optional<A> current;

    private RandomDataGenerator(DataGenerator<A> source, double density, int initialSlice) {
        this.random = new Random();
        this.density = density;
        this.slice = initialSlice;

        Pair<DataGenerator<A>, List<A>> pair = source.getAndApply(initialSlice);
        this.source = pair.getLeft();
        this.items = randomize(pair.getRight(), 0);
        this.position = 0;
        this.scaleUpLevel = (int) (items.size() * density);
        this.current = Optional.of(items.get(0));
    }

    private RandomDataGenerator(RandomDataGenerator<A> previousGen, int position) {
        this.random = previousGen.random;
        this.density = previousGen.density;

        if (previousGen.source.isClosed() || position < previousGen.scaleUpLevel) {
            this.items = previousGen.items;
            this.scaleUpLevel = previousGen.scaleUpLevel;
            this.slice = previousGen.slice;

            if (position < items.size()) {
                this.source = previousGen.source;
                this.current = Optional.of(items.get(position));
            } else {
                this.source = (DataGenerator<A>) EOS;
                this.current = Optional.empty();
            }
        } else {
            this.slice = previousGen.slice * 2;

            Pair<DataGenerator<A>, List<A>> pair = resize(previousGen.source, previousGen.items, slice);
            this.source = pair.getLeft();
            this.items = randomize(pair.getRight(), position);
            this.scaleUpLevel = (int) (items.size() * density);
            this.current = Optional.of(items.get(position));
        }

        this.position = position;
    }

    @Override
    public DataGenerator<A> apply() {
        RandomDataGenerator<A> nextGen = new RandomDataGenerator<>(this, position + 1);
        return nextGen.isClosed() ? (DataGenerator<A>) EOS : nextGen;
    }

    @Override
    public Optional<A> getOptionalValue() {
        return current;
    }

    private List<A> randomize(List<A> items, int randomizeFromIdx) {
        int outputSize = items.size();
        int last = outputSize - 1;
        for (int i = randomizeFromIdx; i < last; i++) {
            int rpos = i + random.nextInt(outputSize - i);
            A tmp = items.get(rpos);
            items.set(rpos, items.get(i));
            items.set(i, tmp);
        }
        return items;
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> source) {
        return newInstance(source, DEFAULT_DENSITY, DEFAULT_INITIAL_CHUNK_SIZE);
    }

    public static <A> DataGenerator<A> newInstance(DataGenerator<A> source, double density, int initialChunkSize) {
        if (source.isClosed()) {
            return (DataGenerator<A>) EOS;
        }
        return new RandomDataGenerator<>(source, density, initialChunkSize);
    }

    private static <A> Pair<DataGenerator<A>, List<A>> resize(DataGenerator<A> source, List<A> items, int slice) {
        List<A> resized = new ArrayList<>(slice);
        resized.addAll(items);
        return Pair.of(source.apply(resized::add, slice - items.size()), resized);
    }
}
