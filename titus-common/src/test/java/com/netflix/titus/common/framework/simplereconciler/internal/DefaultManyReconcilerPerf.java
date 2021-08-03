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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.framework.simplereconciler.ManyReconciler;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultManyReconcilerPerf {

    private static final int SHARD_COUNT = 10;
    private static final int ENGINE_COUNT = 2000;
    private static final int LIMIT = 10;

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final ManyReconciler<Integer> reconciler;
    private final AtomicBoolean initialized = new AtomicBoolean();

    private Disposable eventSubscription;

    public DefaultManyReconcilerPerf() {
        ReconcilerActionProviderPolicy internalProviderPolicy = ReconcilerActionProviderPolicy.newBuilder()
                .withName("slow")
                .withPriority(ReconcilerActionProviderPolicy.DEFAULT_INTERNAL_ACTIONS_PRIORITY)
                .build();
        Function<Integer, List<Mono<Function<Integer, Integer>>>> internalActionsProvider = data -> {
            if (!initialized.get()) {
                return Collections.emptyList();
            }
            long delayMs = Math.max(1, System.nanoTime() % 10);
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignore) {
            }
            return Collections.singletonList(Mono.just(current -> current + 1));
        };

        this.reconciler = ManyReconciler.<Integer>newBuilder()
                .withName("test")
                .withShardCount(SHARD_COUNT)
                .withQuickCycle(Duration.ofMillis(1))
                .withLongCycle(Duration.ofMillis(1))
                .withExternalActionProviderPolicy(ReconcilerActionProviderPolicy.getDefaultExternalPolicy())
                .withReconcilerActionsProvider(internalProviderPolicy, internalActionsProvider)
                .withTitusRuntime(titusRuntime)
                .build();
    }

    private void load(int engineCount) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        List<Mono<Void>> action = new ArrayList<>();
        for (int i = 0; i < engineCount; i++) {
            action.add(reconciler.add("#" + i, i));
        }
        Flux.merge(action).collectList().block();
        System.out.println("Created jobs in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        initialized.set(true);
    }

    private void run(int limit) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        this.eventSubscription = reconciler.changes().subscribe(
                events -> {
//                    events.forEach(event -> {
//                        System.out.println("EngineId=" + event.getId() + ", DataVersion=" + event.getData());
//                    });
                }
        );
        System.out.println("Engines to watch: " + reconciler.getAll().size());
        while (!haveAllEnginesReachedLimit(limit)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {
            }
        }
        System.out.println("All engines reached limit " + limit + " in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }

    private void stop() {
        eventSubscription.dispose();
        reconciler.close().block();
    }

    private boolean haveAllEnginesReachedLimit(int limit) {
        for (int counter : reconciler.getAll().values()) {
            if (counter < limit) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        DefaultManyReconcilerPerf perf = new DefaultManyReconcilerPerf();
        perf.load(ENGINE_COUNT);
        perf.run(LIMIT);
        perf.stop();
    }
}
