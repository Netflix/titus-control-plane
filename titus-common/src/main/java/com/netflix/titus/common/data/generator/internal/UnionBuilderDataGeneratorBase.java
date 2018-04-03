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

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.CollectionsExt;

import static java.util.Arrays.asList;

public abstract class UnionBuilderDataGeneratorBase<BUILDER> extends DataGenerator.BuilderDataGenerator2<BUILDER> {

    private static final UnionBuilderDataGeneratorBase<?> EMPTY_BIND_BUILDER_GENERATOR = new EmptyBuilderDataGenerator<>();

    protected abstract List<Object> values();

    public static <BUILDER> DataGenerator.BuilderDataGenerator2<BUILDER> newInstance(Supplier<BUILDER> builderSupplier) {
        return new InitialBindBuilderDataGenerator<>(builderSupplier);
    }

    static class EmptyBuilderDataGenerator<BUILDER> extends UnionBuilderDataGeneratorBase<BUILDER> {

        @Override
        public <B> BuilderDataGenerator2<BUILDER> combine(DataGenerator<B> source, BiConsumer<BUILDER, B> builderSetter) {
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

    static class InitialBindBuilderDataGenerator<BUILDER> extends UnionBuilderDataGeneratorBase<BUILDER> {

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
        public <B> BuilderDataGenerator2<BUILDER> combine(DataGenerator<B> source, BiConsumer<BUILDER, B> builderSetter) {
            return source.getOptionalValue().isPresent()
                    ? new OneValueBindBuilderDataGenerator<>(builderSupplier, source, builderSetter)
                    : (BuilderDataGenerator2<BUILDER>) EMPTY_BIND_BUILDER_GENERATOR;
        }

        @Override
        protected List<Object> values() {
            throw new IllegalStateException("No more values");
        }
    }

    static class OneValueBindBuilderDataGenerator<A, BUILDER, CONTEXT> extends UnionBuilderDataGeneratorBase<BUILDER> {

        private final Supplier<BUILDER> builderSupplier;
        private final DataGenerator<A> domain;
        private final BiConsumer<BUILDER, A> firstBuilderSetter;

        private final List<Object> current;

        OneValueBindBuilderDataGenerator(Supplier<BUILDER> builderSupplier,
                                         DataGenerator<A> domain,
                                         BiConsumer<BUILDER, A> firstBuilderSetter) {
            this.builderSupplier = builderSupplier;
            this.domain = domain;
            this.firstBuilderSetter = firstBuilderSetter;
            this.current = Collections.singletonList(domain.getValue());
        }

        @Override
        public UnionBuilderDataGeneratorBase<BUILDER> apply() {
            DataGenerator<A> nextDomain = domain.apply();
            if (nextDomain.isClosed()) {
                return (UnionBuilderDataGeneratorBase<BUILDER>) EMPTY_BIND_BUILDER_GENERATOR;
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
        public <B> BuilderDataGenerator2<BUILDER> combine(DataGenerator<B> source, BiConsumer<BUILDER, B> builderSetter) {
            return new MultiValueBindBuilderDataGenerator<>(
                    this,
                    source,
                    source,
                    builderSupplier,
                    asList((BiConsumer<BUILDER, Object>) firstBuilderSetter, (BiConsumer<BUILDER, Object>) builderSetter)
            );
        }

        @Override
        public List<Object> values() {
            return current;
        }
    }

    static class MultiValueBindBuilderDataGenerator<B, BUILDER> extends UnionBuilderDataGeneratorBase<BUILDER> {
        private final UnionBuilderDataGeneratorBase<BUILDER> domain;

        private final DataGenerator<B> initialCodomain;
        private final DataGenerator<B> codomain;

        private final List<Object> current;

        private final Supplier<BUILDER> builderSupplier;
        private final List<BiConsumer<BUILDER, Object>> builderSetters;

        MultiValueBindBuilderDataGenerator(UnionBuilderDataGeneratorBase<BUILDER> domain,
                                           DataGenerator<B> initialCodomain,
                                           DataGenerator<B> codomain,
                                           Supplier<BUILDER> builderSupplier,
                                           List<BiConsumer<BUILDER, Object>> builderSetters) {
            this.initialCodomain = initialCodomain;
            this.domain = domain;
            this.builderSupplier = builderSupplier;
            this.builderSetters = builderSetters;

            List<Object> a = domain.values();
            this.current = CollectionsExt.copyAndAdd(a, codomain.getValue());
            this.codomain = codomain;
        }

        @Override
        public DataGenerator<BUILDER> apply() {
            DataGenerator<B> newCodomain = codomain.apply();
            if (!newCodomain.isClosed()) {
                return new MultiValueBindBuilderDataGenerator<>(
                        domain,
                        initialCodomain,
                        newCodomain,
                        builderSupplier,
                        builderSetters
                );
            }

            DataGenerator<BUILDER> newDomain = domain.apply();
            if (newDomain.isClosed()) {
                return (DataGenerator<BUILDER>) EMPTY_BIND_BUILDER_GENERATOR;
            }

            return new MultiValueBindBuilderDataGenerator<>(
                    (UnionBuilderDataGeneratorBase<BUILDER>) newDomain,
                    initialCodomain,
                    initialCodomain,
                    builderSupplier,
                    builderSetters
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
        public List<Object> values() {
            return current;
        }

        @Override
        public <C> UnionBuilderDataGeneratorBase<BUILDER> combine(DataGenerator<C> source, BiConsumer<BUILDER, C> builderSetter) {
            return new MultiValueBindBuilderDataGenerator<>(
                    this,
                    source,
                    source,
                    builderSupplier,
                    CollectionsExt.copyAndAdd(builderSetters, (BiConsumer<BUILDER, Object>) builderSetter)
            );
        }
    }
}
