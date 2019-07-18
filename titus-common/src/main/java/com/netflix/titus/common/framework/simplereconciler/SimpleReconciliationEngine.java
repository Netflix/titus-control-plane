/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.internal.DefaultSimpleReconciliationEngine;
import com.netflix.titus.common.runtime.TitusRuntime;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * A simple reconciliation engine, which serializes client and internal/reconciler actions that work over the same
 * data item.
 */
public interface SimpleReconciliationEngine<DATA> extends Closeable {

    DATA getCurrent();

    Mono<DATA> apply(Mono<Function<DATA, DATA>> action);

    Flux<DATA> changes();

    static <DATA> Builder<DATA> newBuilder(String name) {
        return new Builder<>(name);
    }

    class Builder<DATA> {

        private final String name;
        private DATA initial;
        private Duration quickCycle;
        private Duration longCycle;
        private Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider;
        private Scheduler scheduler;
        private TitusRuntime titusRuntime;

        private Builder(String name) {
            this.name = name;
        }

        public Builder<DATA> withInitial(DATA initial) {
            this.initial = initial;
            return this;
        }

        /**
         * Monitoring interval for running actions.
         */
        public Builder<DATA> withQuickCycle(Duration quickCycle) {
            this.quickCycle = quickCycle;
            return this;
        }

        /**
         * Monitoring interval at which reconciliation function is run. The interval value should be a multiplication of
         * {@link #withQuickCycle(Duration)} value.
         */
        public Builder<DATA> withLongCycle(Duration longCycle) {
            this.longCycle = longCycle;
            return this;
        }

        public Builder<DATA> withReconcilerActionsProvider(Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider) {
            this.reconcilerActionsProvider = reconcilerActionsProvider;
            return this;
        }

        public Builder<DATA> withScheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder<DATA> withTitusRuntime(TitusRuntime titusRuntime) {
            this.titusRuntime = titusRuntime;
            return this;
        }

        public SimpleReconciliationEngine<DATA> build() {
            return new DefaultSimpleReconciliationEngine<>(name, initial, quickCycle, longCycle, reconcilerActionsProvider, scheduler, titusRuntime);
        }
    }
}
