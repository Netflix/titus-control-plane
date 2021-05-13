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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.internal.DefaultManyReconciler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.closeable.CloseableReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A simple reconciliation framework that manages multiple data items. Each individual data item is processed
 * independently, with no concurrency constraints.
 */
public interface ManyReconciler<DATA> {

    Mono<Void> add(String id, DATA initial);

    Mono<Void> remove(String id);

    Mono<Void> close();

    Map<String, DATA> getAll();

    Optional<DATA> findById(String id);

    Mono<DATA> apply(String id, Function<DATA, Mono<DATA>> action);

    Flux<List<SimpleReconcilerEvent<DATA>>> changes();

    static <DATA> Builder<DATA> newBuilder() {
        return new Builder<>();
    }

    class Builder<DATA> {

        private String name = "default";
        private Duration quickCycle;
        private Duration longCycle;
        private Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider;
        private Scheduler reconcilerScheduler;
        private Scheduler notificationScheduler;
        private TitusRuntime titusRuntime;

        private Builder() {
        }

        /**
         * Reconciler unique name (for reporting purposes).
         */
        public Builder<DATA> withName(String name) {
            this.name = name;
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

        public Builder<DATA> withExternalActionProviderPolicy(ReconcilerActionProviderPolicy policy) {
            return this;
        }

        public Builder<DATA> withReconcilerActionsProvider(Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider) {
            this.reconcilerActionsProvider = reconcilerActionsProvider;
            return this;
        }

        public Builder<DATA> withReconcilerActionsProvider(ReconcilerActionProviderPolicy policy,
                                                           Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider) {
            this.reconcilerActionsProvider = reconcilerActionsProvider;
            return this;
        }

        public Builder<DATA> withReconcilerScheduler(Scheduler reconcilerScheduler) {
            this.reconcilerScheduler = reconcilerScheduler;
            return this;
        }

        public Builder<DATA> withNotificationScheduler(Scheduler notificationScheduler) {
            this.notificationScheduler = notificationScheduler;
            return this;
        }

        public Builder<DATA> withTitusRuntime(TitusRuntime titusRuntime) {
            this.titusRuntime = titusRuntime;
            return this;
        }

        public ManyReconciler<DATA> build() {
            Preconditions.checkNotNull(name, "Name is null");

            CloseableReference<Scheduler> reconcilerSchedulerRef = reconcilerScheduler == null
                    ? CloseableReference.referenceOf(Schedulers.newSingle("reconciler-internal-" + name, true), Scheduler::dispose)
                    : CloseableReference.referenceOf(reconcilerScheduler);

            CloseableReference<Scheduler> notificationSchedulerRef = notificationScheduler == null
                    ? CloseableReference.referenceOf(Schedulers.newSingle("reconciler-notification-" + name, true), Scheduler::dispose)
                    : CloseableReference.referenceOf(notificationScheduler);

            return new DefaultManyReconciler<>(
                    name,
                    quickCycle,
                    longCycle,
                    reconcilerActionsProvider,
                    reconcilerSchedulerRef,
                    notificationSchedulerRef,
                    titusRuntime
            );
        }
    }
}
