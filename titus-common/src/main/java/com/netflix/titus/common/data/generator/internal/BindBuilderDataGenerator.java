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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.CollectionsExt;

import static java.util.Arrays.asList;

public abstract class BindBuilderDataGenerator<BUILDER> extends DataGenerator.BuilderDataGenerator<BUILDER> {

    private static final BindBuilderDataGenerator<?> EMPTY_BIND_BUILDER_GENERATOR = new EmptyBindBuilderDataGenerator<>();

    protected abstract List<Object> values();

    public static <BUILDER> BindBuilderDataGenerator<BUILDER> newInstance(Supplier<BUILDER> builderSupplier) {
        return new InitialBindBuilderDataGenerator<>(builderSupplier);
    }

    private static class EmptyBindBuilderDataGenerator<BUILDER> extends BindBuilderDataGenerator<BUILDER> {

        @Override
        public <B> BindBuilderDataGenerator<BUILDER> bind(DataGenerator<B> codomain, BiConsumer<BUILDER, B> builderSetter) {
            return this;
        }

        @Override
        protected List<Object> values() {
            throw new IllegalStateException("No more values");
        }

        @Override
        public DataGenerator<BUILDER> apply() {
            return this;
        }

        @Override
        public Optional<BUILDER> getOptionalValue() {
            return Optional.empty();
        }
    }

    private static class InitialBindBuilderDataGenerator<BUILDER> extends BindBuilderDataGenerator<BUILDER> {
        private final Supplier<BUILDER> builderSupplier;

        InitialBindBuilderDataGenerator(Supplier<BUILDER> builderSupplier) {
            this.builderSupplier = builderSupplier;
        }

        @Override
        public DataGenerator<BUILDER> apply() {
            return DataGenerator.empty();
        }

        @Override
        public Optional<BUILDER> getOptionalValue() {
            return Optional.empty();
        }

        @Override
        public <B> BindBuilderDataGenerator<BUILDER> bind(DataGenerator<B> codomain, BiConsumer<BUILDER, B> builderSetter) {
            return codomain.getOptionalValue().isPresent()
                    ? new OneValueBindBuilderDataGenerator<>(builderSupplier, codomain, builderSetter)
                    : (BindBuilderDataGenerator<BUILDER>) EMPTY_BIND_BUILDER_GENERATOR;
        }

        @Override
        protected List<Object> values() {
            throw new IllegalStateException("No more values");
        }
    }

    private static class OneValueBindBuilderDataGenerator<A, BUILDER> extends BindBuilderDataGenerator<BUILDER> {

        private final Supplier<BUILDER> builderSupplier;
        private final DataGenerator<A> domain;
        private final BiConsumer<BUILDER, A> firstBuilderSetter;

        private final List<Object> current;

        public OneValueBindBuilderDataGenerator(Supplier<BUILDER> builderSupplier,
                                                DataGenerator<A> domain,
                                                BiConsumer<BUILDER, A> firstBuilderSetter) {
            this.builderSupplier = builderSupplier;
            this.domain = domain;
            this.firstBuilderSetter = firstBuilderSetter;
            this.current = Collections.singletonList(domain.getValue());
        }

        @Override
        public BindBuilderDataGenerator<BUILDER> apply() {
            DataGenerator<A> nextDomain = domain.apply();
            if (nextDomain.isClosed()) {
                return (BindBuilderDataGenerator<BUILDER>) EMPTY_BIND_BUILDER_GENERATOR;
            }
            return new OneValueBindBuilderDataGenerator<>(builderSupplier, nextDomain, firstBuilderSetter);
        }

        @Override
        public Optional<BUILDER> getOptionalValue() {
            List<Object> attributes = values();
            BUILDER newBuilder = builderSupplier.get();
            firstBuilderSetter.accept(newBuilder, (A) attributes.get(0));
            return Optional.of(newBuilder);
        }

        @Override
        public <B> BindBuilderDataGenerator<BUILDER> bind(DataGenerator<B> codomain, BiConsumer<BUILDER, B> builderSetter) {
            return new MultiValueBindBuilderDataGenerator<>(
                    this,
                    codomain,
                    builderSupplier,
                    asList((BiConsumer<BUILDER, Object>) firstBuilderSetter, (BiConsumer<BUILDER, Object>) builderSetter),
                    ImmutableMap.of()
            );
        }

        @Override
        public List<Object> values() {
            return current;
        }
    }

    private static class MultiValueBindBuilderDataGenerator<B, BUILDER> extends BindBuilderDataGenerator<BUILDER> {
        private final BindBuilderDataGenerator<BUILDER> domain;
        private final DataGenerator<B> codomain;

        private final ImmutableMap<List<Object>, B> mappings;
        private final List<Object> current;

        private final Supplier<BUILDER> builderSupplier;
        private final List<BiConsumer<BUILDER, Object>> builderSetters;

        private MultiValueBindBuilderDataGenerator(BindBuilderDataGenerator<BUILDER> domain,
                                                   DataGenerator<B> codomain,
                                                   Supplier<BUILDER> builderSupplier,
                                                   List<BiConsumer<BUILDER, Object>> builderSetters,
                                                   ImmutableMap<List<Object>, B> mappings) {
            this.domain = domain;
            this.codomain = codomain;
            this.builderSupplier = builderSupplier;
            this.builderSetters = builderSetters;
            List<Object> a = domain.values();
            B b = mappings.get(a);
            if (b == null) {
                b = codomain.getValue();
                this.mappings = ImmutableMap.<List<Object>, B>builder().putAll(mappings).put(a, b).build();
            } else {
                this.mappings = mappings;
            }
            this.current = CollectionsExt.copyAndAdd(a, b);
        }

        @Override
        public DataGenerator<BUILDER> apply() {
            DataGenerator<BUILDER> newDomain = domain.apply();
            DataGenerator<B> newCodomain = codomain.apply();
            if (newDomain.isClosed() || newCodomain.isClosed()) {
                return (DataGenerator<BUILDER>) EMPTY_BIND_BUILDER_GENERATOR;
            }
            return new MultiValueBindBuilderDataGenerator<>(
                    (BindBuilderDataGenerator<BUILDER>) newDomain,
                    newCodomain,
                    builderSupplier,
                    builderSetters,
                    mappings
            );
        }

        @Override
        public Optional<BUILDER> getOptionalValue() {
            List<Object> attributes = values();
            BUILDER newBuilder = builderSupplier.get();
            for (int i = 0; i < attributes.size(); i++) {
                builderSetters.get(i).accept(newBuilder, attributes.get(i));
            }
            return Optional.of(newBuilder);
        }

        @Override
        public <C> BindBuilderDataGenerator<BUILDER> bind(DataGenerator<C> codomain, BiConsumer<BUILDER, C> builderSetter) {
            return new MultiValueBindBuilderDataGenerator<>(
                    this,
                    codomain,
                    builderSupplier,
                    CollectionsExt.copyAndAdd(builderSetters, (BiConsumer<BUILDER, Object>) builderSetter),
                    ImmutableMap.of()
            );
        }

        @Override
        public List<Object> values() {
            return current;
        }
    }
}