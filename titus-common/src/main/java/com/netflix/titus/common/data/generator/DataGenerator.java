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

package com.netflix.titus.common.data.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.netflix.titus.common.data.generator.internal.BatchDataGenerator;
import com.netflix.titus.common.data.generator.internal.BindBuilderDataGenerator;
import com.netflix.titus.common.data.generator.internal.BindDataGenerator;
import com.netflix.titus.common.data.generator.internal.CharRangeDataGenerator;
import com.netflix.titus.common.data.generator.internal.ConcatDataGenerator;
import com.netflix.titus.common.data.generator.internal.DataGeneratorOperators;
import com.netflix.titus.common.data.generator.internal.FilterDataGenerator;
import com.netflix.titus.common.data.generator.internal.FlatMappedDataGenerator;
import com.netflix.titus.common.data.generator.internal.FromContextGenerator;
import com.netflix.titus.common.data.generator.internal.ItemDataGenerator;
import com.netflix.titus.common.data.generator.internal.LimitDataGenerator;
import com.netflix.titus.common.data.generator.internal.LongRangeDataGenerator;
import com.netflix.titus.common.data.generator.internal.LoopedDataGenerator;
import com.netflix.titus.common.data.generator.internal.MappedDataGenerator;
import com.netflix.titus.common.data.generator.internal.MappedWithIndexDataGenerator;
import com.netflix.titus.common.data.generator.internal.MergeDataGenerator;
import com.netflix.titus.common.data.generator.internal.RandomDataGenerator;
import com.netflix.titus.common.data.generator.internal.SeqDataGenerator;
import com.netflix.titus.common.data.generator.internal.SeqNumberDataGenerators;
import com.netflix.titus.common.data.generator.internal.UnionBuilderDataGeneratorBase;
import com.netflix.titus.common.data.generator.internal.UnionDataGenerator;
import com.netflix.titus.common.data.generator.internal.ZipDataGenerator;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.tuple.Quadruple;
import com.netflix.titus.common.util.tuple.Triple;
import rx.functions.Func3;

import static java.util.Arrays.asList;

/**
 * TODO Partial objects generation and merging
 */
public abstract class DataGenerator<A> {

    public static abstract class BuilderDataGenerator<BUILDER> extends DataGenerator<BUILDER> {
        public abstract <B> BuilderDataGenerator<BUILDER> bind(DataGenerator<B> codomain, BiConsumer<BUILDER, B> builderSetter);
    }

    public static abstract class BuilderDataGenerator2<BUILDER> extends DataGenerator<BUILDER> {
        public abstract <B> BuilderDataGenerator2<BUILDER> combine(DataGenerator<B> codomain, BiConsumer<BUILDER, B> builderSetter);
    }

    public abstract DataGenerator<A> apply();

    public DataGenerator<A> apply(Consumer<A> consumer) {
        getOptionalValue().ifPresent(consumer);
        return apply();
    }

    public DataGenerator<A> apply(Consumer<A> consumer, int times) {
        DataGenerator<A> nextGen = this;
        for (int i = 0; i < times && !nextGen.isClosed(); i++) {
            consumer.accept(nextGen.getValue());
            nextGen = nextGen.apply();
        }
        return nextGen;
    }

    public Pair<DataGenerator<A>, List<A>> getAndApply(int times) {
        List<A> items = new ArrayList<>();
        return Pair.of(apply(items::add, times), items);
    }

    public boolean isClosed() {
        return !getOptionalValue().isPresent();
    }

    public A getValue() {
        return getOptionalValue().orElseThrow(() -> new IllegalStateException("No more values"));
    }

    public List<A> getValues(int count) {
        return getAndApply(count).getRight();
    }

    public abstract Optional<A> getOptionalValue();

    public <B> DataGenerator<B> cast(Class<B> newType) {
        return (DataGenerator<B>) this;
    }

    /**
     * Generate batches of data.
     */
    public DataGenerator<List<A>> batch(int batchSize) {
        return BatchDataGenerator.newInstance(this, batchSize);
    }

    /**
     * Create infinite data generator, repeating the same data sequence.
     */
    public DataGenerator<A> loop() {
        return LoopedDataGenerator.newInstance(this);
    }

    /**
     * Create infinite data generator, repeating the same data sequence, appending to each item the iteration index.
     */
    public DataGenerator<A> loop(BiFunction<A, Integer, A> decorator) {
        return LoopedDataGenerator.newInstance(this, decorator);
    }

    /**
     * Join two generated streams one after another.
     */
    public DataGenerator<A> concat(DataGenerator<A> another) {
        return ConcatDataGenerator.newInstance(this, another);
    }

    /**
     * Emit items matching a given predicate.
     */
    public DataGenerator<A> filter(Function<A, Boolean> predicate) {
        return FilterDataGenerator.newInstance(this, predicate);
    }

    public <B> DataGenerator<B> flatMap(Function<A, DataGenerator<B>> transformer) {
        return FlatMappedDataGenerator.newInstance(this, transformer);
    }

    /**
     * Limit data stream to a given number of items.
     */
    public DataGenerator<A> limit(int count) {
        return LimitDataGenerator.newInstance(this, count);
    }

    public <B> DataGenerator<B> map(Function<A, B> transformer) {
        return MappedDataGenerator.newInstance(this, transformer);
    }

    public <B> DataGenerator<B> map(BiFunction<Long, A, B> transformer) {
        return MappedWithIndexDataGenerator.newInstance(this, transformer);
    }

    public DataGenerator<A> random() {
        return RandomDataGenerator.newInstance(this);
    }

    public DataGenerator<A> random(double density, int initialChunkSize) {
        return RandomDataGenerator.newInstance(this, density, initialChunkSize);
    }

    /**
     * Skip a given number of items.
     */
    public DataGenerator<A> skip(int number) {
        DataGenerator<A> next = this;
        for (int i = 0; i < number; i++) {
            if (isClosed()) {
                return (DataGenerator<A>) EOS;
            }
            next = next.apply();
        }
        return next;
    }

    public List<A> toList() {
        return DataGeneratorOperators.toList(this);
    }

    public List<A> toList(int limit) {
        return DataGeneratorOperators.toList(this.limit(limit));
    }

    public static <BUILDER> BuilderDataGenerator<BUILDER> bindBuilder(Supplier<BUILDER> builderSupplier) {
        return BindBuilderDataGenerator.newInstance(builderSupplier);
    }

    /**
     * For each item from 'source' assign exactly one item from 'into'. If the same value is repeated in the
     * 'source' generator, the original assignment is returned.
     */
    public static <A, B> DataGenerator<Pair<A, B>> bind(DataGenerator<A> source, DataGenerator<B> into) {
        return BindDataGenerator.newInstance(source, into);
    }

    /**
     * For each item from 'source' assign one item from 'ontoB', and next one item from 'ontoC'. If the same value is repeated in the
     * 'source' generator, the original triple value is returned.
     */
    public static <A, B, C> DataGenerator<Triple<A, B, C>> bind(DataGenerator<A> source, DataGenerator<B> intoB, DataGenerator<C> intoC) {
        return BindDataGenerator.newInstance(BindDataGenerator.newInstance(source, intoB), intoC).map(
                abc -> Triple.of(abc.getLeft().getLeft(), abc.getLeft().getRight(), abc.getRight())
        );
    }

    /**
     * Three level binding. See {@link #bind(DataGenerator, DataGenerator)}.
     */
    public static <A, B, C, D> DataGenerator<Quadruple<A, B, C, D>> bind(DataGenerator<A> source, DataGenerator<B> intoB, DataGenerator<C> intoC, DataGenerator<D> intoD) {
        return bind(bind(source, intoB, intoC), intoD).map(abcd ->
                Quadruple.of(abcd.getLeft().getFirst(), abcd.getLeft().getSecond(), abcd.getLeft().getThird(), abcd.getRight())
        );
    }

    /**
     * Concatenates values from multiple sources into a string. For each emission, a new value from each source generator is taken.
     */
    public static DataGenerator<String> concatenations(String separator, DataGenerator<String>... sources) {
        return concatenations(separator, asList(sources));
    }

    /**
     * Concatenates values from multiple sources into a string. For each emission, a new value from each source generator is taken.
     */
    public static DataGenerator<String> concatenations(String separator, List<DataGenerator<String>> sources) {
        return union(sources, "", (acc, v) -> acc.isEmpty() ? v : acc + separator + v);
    }

    public static <V> DataGenerator<V> empty() {
        return (DataGenerator<V>) EOS;
    }

    /**
     * Generate sequence of data based on a context. Stream is terminated when the generated optional value is empty.
     */
    public static <CONTEXT, V> DataGenerator<V> fromContextOpt(CONTEXT initial, Function<CONTEXT, Pair<CONTEXT, Optional<V>>> valueSupplier) {
        return FromContextGenerator.newInstance(initial, valueSupplier);
    }

    /**
     * Generate sequence of data based on a context.
     */
    public static <CONTEXT, V> DataGenerator<V> fromContext(CONTEXT initial, Function<CONTEXT, Pair<CONTEXT, V>> valueSupplier) {
        return FromContextGenerator.newInstance(initial, context -> valueSupplier.apply(context).mapRight(Optional::ofNullable));
    }

    public static <A> DataGenerator<A> items(A... words) {
        return new ItemDataGenerator(asList(words), 0);
    }

    public static <A> DataGenerator<A> items(List<A> words) {
        return new ItemDataGenerator(words, 0);
    }

    public static DataGenerator<Character> range(char from, char to) {
        return new CharRangeDataGenerator(from, to, from);
    }

    public static DataGenerator<Long> range(long from, long to) {
        return new LongRangeDataGenerator(from, to, from);
    }

    public static DataGenerator<Integer> rangeInt(int from, int to) {
        return new LongRangeDataGenerator(from, to, from).map(Math::toIntExact);
    }

    public static DataGenerator<Long> range(long from) {
        return new LongRangeDataGenerator(from, Long.MAX_VALUE, from);
    }

    /**
     * Multiplexes input from multiple data generators into single stream.
     */
    public static <A> DataGenerator<A> merge(DataGenerator<A>... sources) {
        return MergeDataGenerator.newInstance(asList(sources));
    }

    /**
     * Multiplexes input from multiple data generators into single stream.
     */
    public static <A> DataGenerator<A> merge(Collection<DataGenerator<A>> sources) {
        return MergeDataGenerator.newInstance(new ArrayList<>(sources));
    }

    /**
     * Creates batches of data using a provided factory method. When producer emits empty optional value, the stream
     * is terminated.
     */
    public static <CONTEXT, A> DataGenerator<A[]> sequenceOpt(CONTEXT context,
                                                              int size,
                                                              Function<CONTEXT, Pair<CONTEXT, Optional<A>>> producer) {
        return SeqDataGenerator.newInstance(context, size, producer);
    }

    /**
     * Creates batches of data using a provided factory method.
     */
    public static <CONTEXT, A> DataGenerator<A[]> sequence(CONTEXT context,
                                                           int size,
                                                           Function<CONTEXT, Pair<CONTEXT, A>> producer) {
        return SeqDataGenerator.newInstance(context, size, ctx -> producer.apply(ctx).mapRight(Optional::ofNullable));
    }

    /**
     * Creates a stream of string values.
     */
    public static <CONTEXT> DataGenerator<String> sequenceOfString(CONTEXT context,
                                                                   int size,
                                                                   Function<CONTEXT, Pair<CONTEXT, Character>> producer) {
        return sequence(context, size, producer).map(chars -> new String(CollectionsExt.toPrimitiveCharArray(chars)));
    }

    /**
     * Generates sequences of ascending numbers (a1, a2, a3, ..., aN), where N is the sequence size, a1 >= lowerBound,
     * and aN <= upperBound.
     */
    public static DataGenerator<int[]> sequenceOfAscendingIntegers(int size, int lowerBound, int upperBound) {
        return SeqNumberDataGenerators.sequenceOfAscendingIntegers(size, lowerBound, upperBound);
    }

    /**
     * Generates all permutations of data from source generators.
     */
    public static <A> DataGenerator<List<A>> union(DataGenerator<A>... sources) {
        return UnionDataGenerator.newInstance(asList(sources));
    }

    /**
     * Generates all permutations of data from source generators.
     */
    public static <A> DataGenerator<List<A>> union(List<DataGenerator<A>> sources) {
        return UnionDataGenerator.newInstance(sources);
    }

    /**
     * Generates all permutations of data from source generators.
     */
    public static <A, B, R> DataGenerator<R> union(DataGenerator<A> first, DataGenerator<B> second, BiFunction<A, B, R> combiner) {
        List<DataGenerator<Object>> sources = asList((DataGenerator<Object>) first, (DataGenerator<Object>) second);
        return UnionDataGenerator.newInstance(sources).map(list -> combiner.apply((A) list.get(0), (B) list.get(1)));
    }

    /**
     * Generates all permutations of data from source generators.
     */
    public static <A, B, C, R> DataGenerator<R> union(DataGenerator<A> first,
                                                      DataGenerator<B> second,
                                                      DataGenerator<C> third,
                                                      Func3<A, B, C, R> combiner) {
        List<DataGenerator<Object>> sources = asList((DataGenerator<Object>) first, (DataGenerator<Object>) second, (DataGenerator<Object>) third);
        return UnionDataGenerator.newInstance(sources).map(list -> combiner.call((A) list.get(0), (B) list.get(1), (C) list.get(2)));
    }

    /**
     * Generates all permutations of data from source generators.
     */
    public static <A, R> DataGenerator<R> union(List<DataGenerator<A>> sources, R zero, BiFunction<R, A, R> combiner) {
        return UnionDataGenerator.newInstance(sources).map(list -> {
            R result = zero;
            for (A e : list) {
                result = combiner.apply(result, e);
            }
            return result;
        });
    }

    /**
     * Generates all permutations of data from source generators.
     */
    public static <BUILDER> BuilderDataGenerator2<BUILDER> unionBuilder(Supplier<BUILDER> builderSupplier) {
        return UnionBuilderDataGeneratorBase.newInstance(builderSupplier);
    }

    public static <A, B> DataGenerator<Pair<A, B>> zip(DataGenerator<A> first, DataGenerator<B> second) {
        return ZipDataGenerator.newInstance(first, second);
    }

    protected static DataGenerator<?> EOS = new DataGenerator<Object>() {
        @Override
        public DataGenerator<Object> apply() {
            throw new IllegalStateException("No more items");
        }

        @Override
        public Optional<Object> getOptionalValue() {
            return Optional.empty();
        }
    };
}
