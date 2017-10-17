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

import java.util.List;
import java.util.Optional;

import io.netflix.titus.common.data.generator.DataGenerator;

public class ItemDataGenerator<A> extends DataGenerator<A> {

    private final List<A> words;
    private final int currentIdx;
    private final Optional<A> currentValue;

    public ItemDataGenerator(List<A> words, int currentIdx) {
        this.words = words;
        this.currentIdx = currentIdx;
        this.currentValue = Optional.of(words.get(currentIdx));
    }

    @Override
    public DataGenerator<A> apply() {
        int nextIdx = currentIdx + 1;
        if (nextIdx == words.size()) {
            return (DataGenerator<A>) EOS;
        }
        return new ItemDataGenerator(words, nextIdx);
    }

    @Override
    public Optional<A> getOptionalValue() {
        return currentValue;
    }
}
