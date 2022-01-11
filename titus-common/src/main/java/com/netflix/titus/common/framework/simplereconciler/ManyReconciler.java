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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.internal.DefaultManyReconciler;
import com.netflix.titus.common.framework.simplereconciler.internal.ReconcilerExecutorMetrics;
import com.netflix.titus.common.framework.simplereconciler.internal.ShardedManyReconciler;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.ActionProviderSelectorFactory;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.closeable.CloseableReference;
import com.netflix.titus.common.util.collections.index.IndexSet;
import com.netflix.titus.common.util.collections.index.IndexSetHolder;
import com.netflix.titus.common.util.collections.index.IndexSetHolderBasic;
import com.netflix.titus.common.util.collections.index.IndexSetHolderConcurrent;
import com.netflix.titus.common.util.collections.index.Indexes;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy.DEFAULT_EXTERNAL_POLICY_NAME;
import static com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy.getDefaultExternalPolicy;

/**
 * A simple reconciliation framework that manages multiple data items. Each individual data item is processed
 * independently, with no concurrency constraints.
 */
public interface ManyReconciler<DATA> {

    Mono<Void> add(String id, DATA initial);

    Mono<Void> remove(String id);

    Mono<Void> close();

    Map<String, DATA> getAll();

    IndexSet<String, DATA> getIndexSet();

    Optional<DATA> findById(String id);

    int size();

    Mono<DATA> apply(String id, Function<DATA, Mono<DATA>> action);

    Flux<List<SimpleReconcilerEvent<DATA>>> changes(String clientId);

    default Flux<List<SimpleReconcilerEvent<DATA>>> changes() {
        return changes(UUID.randomUUID().toString());
    }

    static <DATA> Builder<DATA> newBuilder() {
        return new Builder<>();
    }

    class Builder<DATA> {

        private String name = "default";
        private Duration quickCycle;
        private Duration longCycle;
        private ReconcilerActionProvider<DATA> externalActionProvider;
        private final Map<String, ReconcilerActionProvider<DATA>> internalActionProviders = new HashMap<>();
        private Scheduler reconcilerScheduler;
        private Scheduler notificationScheduler;
        private TitusRuntime titusRuntime;
        private int shardCount;
        private IndexSet<String, DATA> indexes;

        private Builder() {
        }

        /**
         * Reconciler unique name (for reporting purposes).
         */
        public Builder<DATA> withName(String name) {
            this.name = name;
            return this;
        }

        public Builder<DATA> withShardCount(int shardCount) {
            this.shardCount = shardCount;
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

        public Builder<DATA> withReconcilerScheduler(Scheduler reconcilerScheduler) {
            this.reconcilerScheduler = reconcilerScheduler;
            return this;
        }

        public Builder<DATA> withNotificationScheduler(Scheduler notificationScheduler) {
            this.notificationScheduler = notificationScheduler;
            return this;
        }

        public Builder<DATA> withIndexes(IndexSet<String, DATA> indexes) {
            this.indexes = indexes;
            return this;
        }

        public Builder<DATA> withTitusRuntime(TitusRuntime titusRuntime) {
            this.titusRuntime = titusRuntime;
            return this;
        }

        public ManyReconciler<DATA> build() {
            Preconditions.checkNotNull(name, "Name is null");
            Preconditions.checkNotNull(titusRuntime, "TitusRuntime is null");

            // Indexes are optional. If not set, provide the default value.
            if (indexes == null) {
                indexes = Indexes.empty();
            }

            return shardCount <= 1 ? buildDefaultManyReconciler() : buildShardedManyReconciler();
        }

        private ManyReconciler<DATA> buildDefaultManyReconciler() {
            CloseableReference<Scheduler> reconcilerSchedulerRef = reconcilerScheduler == null
                    ? CloseableReference.referenceOf(Schedulers.newSingle("reconciler-internal-" + name, true), Scheduler::dispose)
                    : CloseableReference.referenceOf(reconcilerScheduler);

            CloseableReference<Scheduler> notificationSchedulerRef = notificationScheduler == null
                    ? CloseableReference.referenceOf(Schedulers.newSingle("reconciler-notification-" + name, true), Scheduler::dispose)
                    : CloseableReference.referenceOf(notificationScheduler);

            IndexSetHolderBasic<String, DATA> indexSetHolder = new IndexSetHolderBasic<>(indexes);
            return new DefaultManyReconciler<>(
                    name,
                    quickCycle,
                    longCycle,
                    buildActionProviderSelectorFactory(),
                    reconcilerSchedulerRef,
                    notificationSchedulerRef,
                    indexSetHolder,
                    buildIndexSetHolderMonitor(indexSetHolder),
                    titusRuntime
            );
        }

        private ManyReconciler<DATA> buildShardedManyReconciler() {
            Function<Integer, CloseableReference<Scheduler>> reconcilerSchedulerSupplier = shardIndex ->
                    CloseableReference.referenceOf(
                            Schedulers.newSingle("reconciler-internal-" + name + "-" + shardIndex, true),
                            Scheduler::dispose
                    );

            CloseableReference<Scheduler> notificationSchedulerRef = notificationScheduler == null
                    ? CloseableReference.referenceOf(Schedulers.newSingle("reconciler-notification-" + name, true), Scheduler::dispose)
                    : CloseableReference.referenceOf(notificationScheduler);

            Function<String, Integer> shardIndexSupplier = id -> Math.abs(id.hashCode()) % shardCount;

            IndexSetHolderConcurrent<String, DATA> indexSetHolder = new IndexSetHolderConcurrent<>(indexes);
            return ShardedManyReconciler.newSharedDefaultManyReconciler(
                    name,
                    shardCount,
                    shardIndexSupplier,
                    quickCycle,
                    longCycle,
                    buildActionProviderSelectorFactory(),
                    reconcilerSchedulerSupplier,
                    notificationSchedulerRef,
                    indexSetHolder,
                    buildIndexSetHolderMonitor(indexSetHolder),
                    titusRuntime
            );
        }

        private Closeable buildIndexSetHolderMonitor(IndexSetHolder<String, DATA> indexSetHolder) {
            return Indexes.monitor(
                    titusRuntime.getRegistry().createId(ReconcilerExecutorMetrics.ROOT_NAME + "indexes"),
                    indexSetHolder,
                    titusRuntime.getRegistry()
            );
        }

        private ActionProviderSelectorFactory<DATA> buildActionProviderSelectorFactory() {
            List<ReconcilerActionProvider<DATA>> actionProviders = new ArrayList<>();
            if (externalActionProvider == null) {
                actionProviders.add(new ReconcilerActionProvider<>(getDefaultExternalPolicy(), true, data -> Collections.emptyList()));
            } else {
                actionProviders.add(externalActionProvider);
            }
            actionProviders.addAll(internalActionProviders.values());
            ActionProviderSelectorFactory<DATA> providerSelector = new ActionProviderSelectorFactory<>(name, actionProviders, titusRuntime);
            return providerSelector;
        }
    }
}
