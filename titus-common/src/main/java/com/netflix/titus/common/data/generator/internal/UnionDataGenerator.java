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
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;

/**
 */
public class UnionDataGenerator<A> extends DataGenerator<List<A>> {

    private static final double DEFAULT_DENSITY = 0.3;
    private static final int DEFAULT_INITIAL_CHUNK_SIZE = 3;

    private final double density;
    private final int slice;
    private final boolean extendable;
    private final List<DataGenerator<A>> sources;

    private final List<List<A>> items;
    private final Combinations combinations;

    private final int position;
    private final int scaleUpLevel;
    private final Optional<List<A>> currentValue;

    private UnionDataGenerator(List<DataGenerator<A>> sources, double density, int initialSlice) {
        this.density = density;
        this.slice = initialSlice;
        this.extendable = true;

        Pair<List<DataGenerator<A>>, List<List<A>>> pair = fill(sources, initialSlice);
        this.sources = pair.getLeft();
        this.items = pair.getRight();

        List<Integer> newSeqSizes = items.stream().map(List::size).collect(Collectors.toList());
        this.combinations = Combinations.newInstance(sources.size()).resize(newSeqSizes);
        this.position = 0;
        this.scaleUpLevel = (int) (combinations.getSize() * density);

        this.currentValue = valueAt(0);
    }

    private UnionDataGenerator(UnionDataGenerator<A> generator, int position) {
        this.density = generator.density;

        if (!generator.extendable || position < generator.scaleUpLevel) {
            this.slice = generator.slice;
            this.extendable = generator.extendable;
            this.scaleUpLevel = generator.scaleUpLevel;
            this.sources = generator.sources;
            this.items = generator.items;
            this.combinations = generator.combinations;
        } else {
            this.slice = generator.slice * 2;
            Optional<Pair<List<DataGenerator<A>>, List<List<A>>>> resizedOpt = resizeSourceData(generator, slice);
            if (resizedOpt.isPresent()) {
                this.extendable = true;
                Pair<List<DataGenerator<A>>, List<List<A>>> resized = resizedOpt.get();
                this.sources = resized.getLeft();
                this.items = resized.getRight();
                this.combinations = resizeCombinations(generator.combinations, items);
                this.scaleUpLevel = (int) (combinations.getSize() * density);
            } else {
                this.extendable = false;
                this.scaleUpLevel = generator.combinations.getSize();
                this.sources = generator.sources;
                this.items = generator.items;
                this.combinations = generator.combinations;
            }
        }

        this.position = position;
        this.currentValue = valueAt(position);
    }

    @Override
    public DataGenerator<List<A>> apply() {
        UnionDataGenerator nextGen = new UnionDataGenerator(this, position + 1);
        return nextGen.isClosed() ? (DataGenerator<List<A>>) EOS : nextGen;
    }

    @Override
    public Optional<List<A>> getOptionalValue() {
        return currentValue;
    }

    private Pair<List<DataGenerator<A>>, List<List<A>>> fill(List<DataGenerator<A>> sources, int sliceSize) {
        List<DataGenerator<A>> nextSources = new ArrayList<>(sources.size());
        List<List<A>> items = new ArrayList<>();
        for (DataGenerator<A> source : sources) {
            Pair<DataGenerator<A>, List<A>> next = fill(source, sliceSize);
            nextSources.add(next.getLeft());
            items.add(next.getRight());
        }
        return Pair.of(nextSources, items);
    }

    private Pair<DataGenerator<A>, List<A>> fill(DataGenerator<A> source, int sliceSize) {
        DataGenerator<A> nextSource = source;
        List<A> values = new ArrayList<>(sliceSize);
        for (int i = 0; i < sliceSize && !nextSource.isClosed(); i++) {
            values.add(nextSource.getValue());
            nextSource = nextSource.apply();
        }
        return Pair.of(nextSource, values);
    }

    private Optional<Pair<List<DataGenerator<A>>, List<List<A>>>> resizeSourceData(UnionDataGenerator<A> previous, int slice) {
        List<DataGenerator<A>> previousSources = previous.sources;
        List<DataGenerator<A>> nextSources = new ArrayList<>(previousSources.size());
        List<List<A>> items = new ArrayList<>();

        int added = 0;
        for (int i = 0; i < previousSources.size(); i++) {
            Pair<DataGenerator<A>, List<A>> pair = fill(previousSources.get(i), slice - previous.items.get(i).size());
            nextSources.add(pair.getLeft());
            added += pair.getRight().size();
            items.add(CollectionsExt.merge(previous.items.get(i), pair.getRight()));
        }

        return added == 0 ? Optional.empty() : Optional.of(Pair.of(nextSources, items));
    }

    private Combinations resizeCombinations(Combinations previousCombinations, List<List<A>> items) {
        List<Integer> newSeqSizes = items.stream().map(List::size).collect(Collectors.toList());
        return previousCombinations.resize(newSeqSizes);
    }

    private Optional<List<A>> valueAt(int position) {
        if (combinations.getSize() <= position) {
            return Optional.empty();
        }
        int[] indexes = combinations.combinationAt(position);
        List<A> tuple = new ArrayList<>(indexes.length);
        for (int i = 0; i < indexes.length; i++) {
            tuple.add(items.get(i).get(indexes[i]));
        }
        return Optional.of(tuple);
    }

    public static <A> DataGenerator<List<A>> newInstance(List<DataGenerator<A>> sources) {
        return newInstance(sources, DEFAULT_DENSITY, DEFAULT_INITIAL_CHUNK_SIZE);
    }

    public static <A> DataGenerator<List<A>> newInstance(List<DataGenerator<A>> sources, double density, int initialSlice) {
        Preconditions.checkArgument(density > 0.0, "Density must be > 0");
        Preconditions.checkArgument(initialSlice > 0, "Initial slice must be > 0");
        if (sources.isEmpty()) {
            return (DataGenerator<List<A>>) EOS;
        }
        return new UnionDataGenerator<>(sources, density, initialSlice);
    }
}
