/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.titus.common.framework.simplereconciler.ManyReconciler;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.framework.simplereconciler.internal.provider.ActionProviderSelectorFactory;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.closeable.CloseableReference;
import com.netflix.titus.common.util.collections.index.IndexSet;
import com.netflix.titus.common.util.collections.index.IndexSetHolder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * A wrapper around a collection of {@link ManyReconciler}s. Shard allocation is based on the managed entity id, which
 * eliminates the need for extra level coordination to prevent the same id to be assigned concurrently to many shards.
 */
public class ShardedManyReconciler<DATA> implements ManyReconciler<DATA> {

    private static final IllegalStateException EXCEPTION_CLOSED = new IllegalStateException("Sharded reconciler closed");

    private final Function<String, Integer> shardIndexSupplier;
    private final CloseableReference<Scheduler> notificationSchedulerRef;
    private final IndexSetHolder<String, DATA> indexSetHolder;
    private final List<ManyReconciler<DATA>> shards;
    private final AtomicReference<ReconcilerState> stateRef = new AtomicReference<>(ReconcilerState.Running);

    public ShardedManyReconciler(int shardCount,
                                 Function<String, Integer> shardIndexSupplier,
                                 Function<Integer, ManyReconciler<DATA>> reconcilerShardFactory,
                                 CloseableReference<Scheduler> notificationSchedulerRef,
                                 IndexSetHolder<String, DATA> indexSetHolder) {
        this.shardIndexSupplier = shardIndexSupplier;
        this.notificationSchedulerRef = notificationSchedulerRef;
        this.indexSetHolder = indexSetHolder;
        List<ManyReconciler<DATA>> shards = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            shards.add(reconcilerShardFactory.apply(i));
        }
        this.shards = shards;
    }

    @Override
    public Mono<Void> add(String id, DATA initial) {
        return Mono.defer(() -> {
            if (stateRef.get() != ReconcilerState.Running) {
                return Mono.error(EXCEPTION_CLOSED);
            }
            return getShard(id).add(id, initial);
        });
    }

    @Override
    public Mono<Void> remove(String id) {
        return Mono.defer(() -> {
            if (stateRef.get() != ReconcilerState.Running) {
                return Mono.error(EXCEPTION_CLOSED);
            }
            return getShard(id).remove(id);
        }).publishOn(notificationSchedulerRef.get());
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (stateRef.get() == ReconcilerState.Closed) {
                return Mono.empty();
            }
            stateRef.compareAndSet(ReconcilerState.Running, ReconcilerState.Closing);
            List<Mono<Void>> closeShardActions = new ArrayList<>();
            for (ManyReconciler<DATA> shard : shards) {
                closeShardActions.add(shard.close());
            }
            return Flux.merge(closeShardActions).ignoreElements()
                    .then(Mono.<Void>empty())
                    .doFinally(signal -> stateRef.set(ReconcilerState.Closed));
        });
    }

    @Override
    public Map<String, DATA> getAll() {
        Map<String, DATA> result = new HashMap<>();
        for (ManyReconciler<DATA> shard : shards) {
            result.putAll(shard.getAll());
        }
        return result;
    }

    @Override
    public IndexSet<String, DATA> getIndexSet() {
        return indexSetHolder.getIndexSet();
    }

    @Override
    public Optional<DATA> findById(String id) {
        return getShard(id).findById(id);
    }

    @Override
    public int size() {
        int sum = 0;
        for (ManyReconciler<DATA> shard : shards) {
            sum += shard.size();
        }
        return sum;
    }

    @Override
    public Mono<DATA> apply(String id, Function<DATA, Mono<DATA>> action) {
        return Mono.defer(() -> {
            if (stateRef.get() != ReconcilerState.Running) {
                return Mono.error(EXCEPTION_CLOSED);
            }
            return getShard(id).apply(id, action);
        }).publishOn(notificationSchedulerRef.get());
    }

    @Override
    public Flux<List<SimpleReconcilerEvent<DATA>>> changes(String clientId) {
        return Flux.defer(() -> {
            if (stateRef.get() != ReconcilerState.Running) {
                return Flux.error(EXCEPTION_CLOSED);
            }
            List<Flux<List<SimpleReconcilerEvent<DATA>>>> shardsChanges = new ArrayList<>();
            shards.forEach(shard -> shardsChanges.add(shard.changes(clientId)));
            return SnapshotMerger.mergeWithSingleSnapshot(shardsChanges);
        }).publishOn(notificationSchedulerRef.get());
    }

    private int computeShardOf(String id) {
        return Math.min(Math.max(0, shardIndexSupplier.apply(id)), shards.size() - 1);
    }

    private ManyReconciler<DATA> getShard(String id) {
        return shards.get(computeShardOf(id));
    }

    public static <DATA> ManyReconciler<DATA> newSharedDefaultManyReconciler(String name,
                                                                             int shardCount,
                                                                             Function<String, Integer> shardIndexSupplier,
                                                                             Duration quickCycle,
                                                                             Duration longCycle,
                                                                             ActionProviderSelectorFactory<DATA> selectorFactory,
                                                                             Function<Integer, CloseableReference<Scheduler>> reconcilerSchedulerSupplier,
                                                                             CloseableReference<Scheduler> notificationSchedulerRef,
                                                                             IndexSetHolder<String, DATA> indexSetHolder,
                                                                             Closeable indexMetricsCloseable,
                                                                             TitusRuntime titusRuntime) {
        return new ShardedManyReconciler<>(
                shardCount,
                shardIndexSupplier,
                shardIndex -> new DefaultManyReconciler<>(
                        name + "#" + shardIndex,
                        quickCycle,
                        longCycle,
                        selectorFactory,
                        reconcilerSchedulerSupplier.apply(shardIndex),
                        notificationSchedulerRef,
                        indexSetHolder,
                        indexMetricsCloseable,
                        titusRuntime
                ),
                notificationSchedulerRef,
                indexSetHolder
        );
    }
}
