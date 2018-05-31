package com.netflix.titus.common.data.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class MutableDataGenerator<A> {

    private final AtomicReference<DataGenerator<A>> dataGeneratorRef;

    public MutableDataGenerator(DataGenerator<A> dataGenerator) {
        this.dataGeneratorRef = new AtomicReference<>(dataGenerator);
    }

    public boolean isClosed() {
        return dataGeneratorRef.get().isClosed();
    }

    public A getValue() {
        DataGenerator<A> current = dataGeneratorRef.get();

        A value = current.getValue();
        dataGeneratorRef.set(current.apply());

        return value;
    }

    public List<A> getValues(int count) {
        List<A> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(getValue());
        }
        return result;
    }

    public Optional<A> getOptionalValue() {
        DataGenerator<A> current = dataGeneratorRef.get();

        Optional<A> value = current.getOptionalValue();
        dataGeneratorRef.set(current.apply());

        return value;
    }
}
