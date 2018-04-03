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

import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;

public class BindDataGenerator<A, B> extends DataGenerator<Pair<A, B>> {

    private final DataGenerator<A> domain;
    private final DataGenerator<B> codomain;

    private final ImmutableMap<A, B> mappings;
    private final Optional<Pair<A, B>> current;

    private BindDataGenerator(DataGenerator<A> domain, DataGenerator<B> codomain, ImmutableMap<A, B> mappings) {
        this.domain = domain;
        this.codomain = codomain;
        A a = domain.getValue();
        B b = mappings.get(a);
        if (b == null) {
            b = codomain.getValue();
            this.mappings = ImmutableMap.<A, B>builder().putAll(mappings).put(a, b).build();
        } else {
            this.mappings = mappings;
        }
        this.current = Optional.of(Pair.of(a, b));
    }

    @Override
    public DataGenerator<Pair<A, B>> apply() {
        return newInstance(domain.apply(), codomain.apply(), mappings);
    }

    @Override
    public Optional<Pair<A, B>> getOptionalValue() {
        return current;
    }

    private static <A, B> DataGenerator<Pair<A, B>> newInstance(DataGenerator<A> domain, DataGenerator<B> codomain, ImmutableMap<A, B> mappings) {
        if (domain.isClosed() || codomain.isClosed()) {
            return (DataGenerator<Pair<A, B>>) EOS;
        }
        return new BindDataGenerator<>(domain, codomain, mappings);
    }

    public static <A, B> DataGenerator<Pair<A, B>> newInstance(DataGenerator<A> domain, DataGenerator<B> codomain) {
        return newInstance(domain, codomain, ImmutableMap.of());
    }
}

