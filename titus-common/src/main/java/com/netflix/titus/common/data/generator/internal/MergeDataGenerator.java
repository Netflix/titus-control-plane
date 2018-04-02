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

import com.netflix.titus.common.data.generator.DataGenerator;

public class MergeDataGenerator<A> extends DataGenerator<A> {

    private final List<DataGenerator<A>> sources;
    private final Optional<A> currentValue;
    private final int currentIdx;

    private MergeDataGenerator(List<DataGenerator<A>> sources, int startFrom) {
        this.sources = sources;
        this.currentIdx = findNext(sources, startFrom);
        this.currentValue = (currentIdx == -1) ? Optional.empty() : sources.get(currentIdx).getOptionalValue();
    }

    @Override
    public DataGenerator<A> apply() {
        if (currentIdx == -1) {
            return (DataGenerator<A>) EOS;
        }
        ArrayList<DataGenerator<A>> genCopy = new ArrayList<>(sources);
        genCopy.set(currentIdx, sources.get(currentIdx).apply());
        return new MergeDataGenerator<>(genCopy, (currentIdx + 1) % genCopy.size());
    }

    @Override
    public Optional<A> getOptionalValue() {
        return currentValue;
    }

    private int findNext(List<DataGenerator<A>> sources, int startFrom) {
        int currentIdx = startFrom;
        do {
            if (!sources.get(currentIdx).isClosed()) {
                return currentIdx;
            }
            currentIdx = (currentIdx + 1) % sources.size();
        } while (currentIdx != startFrom);
        return -1;
    }

    public static <A> DataGenerator<A> newInstance(List<DataGenerator<A>> sources) {
        if (sources.isEmpty()) {
            return (DataGenerator<A>) EOS;
        }
        return new MergeDataGenerator<>(sources, 0);
    }
}
