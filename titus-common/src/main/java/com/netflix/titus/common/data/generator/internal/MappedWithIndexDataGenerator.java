package com.netflix.titus.common.data.generator.internal;

import java.util.Optional;
import java.util.function.BiFunction;

import com.netflix.titus.common.data.generator.DataGenerator;

public class MappedWithIndexDataGenerator<A, B> extends DataGenerator<B> {

    private final DataGenerator<A> sourceGenerator;
    private final BiFunction<Long, A, B> transformer;
    private final long index;
    private final Optional<B> currentValue;

    private MappedWithIndexDataGenerator(DataGenerator<A> sourceGenerator, BiFunction<Long, A, B> transformer, long index) {
        this.sourceGenerator = sourceGenerator;
        this.transformer = transformer;
        this.index = index;
        this.currentValue = sourceGenerator.getOptionalValue().map(t -> transformer.apply(index, t));
    }

    @Override
    public DataGenerator<B> apply() {
        return currentValue.isPresent() ? new MappedWithIndexDataGenerator<>(sourceGenerator.apply(), transformer, index + 1) : (DataGenerator<B>) EOS;
    }

    @Override
    public Optional<B> getOptionalValue() {
        return currentValue;
    }

    public static <A, B> DataGenerator<B> newInstance(DataGenerator<A> sourceGenerator, BiFunction<Long, A, B> transformer) {
        if (sourceGenerator.isClosed()) {
            return (DataGenerator<B>) EOS;
        }
        return new MappedWithIndexDataGenerator<>(sourceGenerator, transformer, 0);
    }
}
