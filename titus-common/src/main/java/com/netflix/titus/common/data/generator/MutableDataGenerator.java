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
