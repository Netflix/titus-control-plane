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

import java.lang.reflect.Array;
import java.util.Optional;
import java.util.function.Function;

import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.common.util.tuple.Pair;

public class SeqDataGenerator<A, CONTEXT> extends DataGenerator<A[]> {

    private final int size;
    private final Function<CONTEXT, Pair<CONTEXT, Optional<A>>> factory;
    private final Pair<CONTEXT, Optional<A[]>> current;

    private SeqDataGenerator(CONTEXT context,
                             int size,
                             Function<CONTEXT, Pair<CONTEXT, Optional<A>>> producer) {
        this.size = size;
        this.factory = producer;
        this.current = makeNext(context, size, producer);
    }

    @Override
    public DataGenerator<A[]> apply() {
        if (current.getRight().isPresent()) {
            return new SeqDataGenerator<>(current.getLeft(), size, factory);
        }
        return (DataGenerator<A[]>) EOS;
    }

    @Override
    public Optional<A[]> getOptionalValue() {
        return current.getRight();
    }

    private Pair<CONTEXT, Optional<A[]>> makeNext(CONTEXT context, int size, Function<CONTEXT, Pair<CONTEXT, Optional<A>>> factory) {
        Pair<CONTEXT, Optional<A>> firstOpt = factory.apply(context);
        if (!firstOpt.getRight().isPresent()) {
            return Pair.of(firstOpt.getLeft(), Optional.empty());
        }
        A first = firstOpt.getRight().get();
        A[] result = (A[]) Array.newInstance(first.getClass(), size);
        result[0] = first;
        CONTEXT nextContext = firstOpt.getLeft();
        for (int i = 1; i < size; i++) {
            Pair<CONTEXT, Optional<A>> nextOpt = factory.apply(nextContext);
            if (!nextOpt.getRight().isPresent()) {
                return Pair.of(nextOpt.getLeft(), Optional.empty());
            }
            result[i] = nextOpt.getRight().get();
            nextContext = nextOpt.getLeft();
        }
        return Pair.of(nextContext, Optional.of(result));
    }

    public static <CONTEXT, A> DataGenerator<A[]> newInstance(CONTEXT context, int size, Function<CONTEXT, Pair<CONTEXT, Optional<A>>> factory) {
        return new SeqDataGenerator<>(context, size, factory);
    }
}
