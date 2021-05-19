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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.internal.DefaultOneOffReconciler;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.ActionProviderSelectorFactory;
import com.netflix.titus.common.runtime.TitusRuntime;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import static com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy.DEFAULT_EXTERNAL_POLICY_NAME;
import static com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy.getDefaultExternalPolicy;

/**
 * A simple reconciliation engine, which serializes client and internal/reconciler actions that work over the same
 * data item.
 */
public interface OneOffReconciler<DATA> {

    Mono<Void> close();

    DATA getCurrent();

    Mono<DATA> apply(Function<DATA, Mono<DATA>> action);

    Flux<DATA> changes();

    static <DATA> Builder<DATA> newBuilder(String id) {
        return new Builder<>(id);
    }

    class Builder<DATA> {

        private final String id;
        private DATA initial;
        private Duration quickCycle;
        private Duration longCycle;
        private ReconcilerActionProvider<DATA> externalActionProvider;
        private Map<String, ReconcilerActionProvider<DATA>> internalActionProviders = new HashMap<>();
        private Scheduler scheduler;
        private TitusRuntime titusRuntime;

        private Builder(String id) {
            this.id = id;
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

        public Builder<DATA> withExternalActionProviderPolicy(ReconcilerActionProviderPolicy policy) {
            // The action provider resolves in this case to an empty list as we handle the external change actions in a different way.
            Preconditions.checkArgument(!internalActionProviders.containsKey(policy.getName()),
                    "External and internal policy names are equal: name=%s", policy.getName());
            this.externalActionProvider = new ReconcilerActionProvider<>(policy, true, data -> Collections.emptyList());
            return this;
        }

        public Builder<DATA> withReconcilerActionsProvider(Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider) {
            withReconcilerActionsProvider(ReconcilerActionProviderPolicy.getDefaultInternalPolicy(), reconcilerActionsProvider);
            return this;
        }

        public Builder<DATA> withReconcilerActionsProvider(ReconcilerActionProviderPolicy policy,
                                                           Function<DATA, List<Mono<Function<DATA, DATA>>>> reconcilerActionsProvider) {
            Preconditions.checkArgument(!policy.getName().equals(DEFAULT_EXTERNAL_POLICY_NAME),
                    "Attempted to use the default external policy name for an internal one: name=%s", DEFAULT_EXTERNAL_POLICY_NAME);
            Preconditions.checkArgument(externalActionProvider == null || !externalActionProvider.getPolicy().getName().equals(policy.getName()),
                    "External and internal policy names must be different: name=%s", policy.getName());
            internalActionProviders.put(policy.getName(), new ReconcilerActionProvider<>(policy, false, reconcilerActionsProvider));
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

        public OneOffReconciler<DATA> build() {
            Preconditions.checkNotNull(id, "id is null");
            Preconditions.checkNotNull(titusRuntime, "TitusRuntime is null");

            List<ReconcilerActionProvider<DATA>> actionProviders = new ArrayList<>();
            if (externalActionProvider == null) {
                actionProviders.add(new ReconcilerActionProvider<>(getDefaultExternalPolicy(), true, data -> Collections.emptyList()));
            } else {
                actionProviders.add(externalActionProvider);
            }
            actionProviders.addAll(internalActionProviders.values());
            ActionProviderSelectorFactory<DATA> providerSelector = new ActionProviderSelectorFactory<>(id, actionProviders, titusRuntime);

            return new DefaultOneOffReconciler<>(
                    id,
                    initial,
                    quickCycle,
                    longCycle,
                    providerSelector,
                    scheduler,
                    titusRuntime
            );
        }
    }
}
