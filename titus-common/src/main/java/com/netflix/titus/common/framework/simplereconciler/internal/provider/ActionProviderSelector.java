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

package com.netflix.titus.common.framework.simplereconciler.internal.provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProvider;
import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProviderPolicy;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.collections.OneItemIterator;
import com.netflix.titus.common.util.time.Clock;

public class ActionProviderSelector<DATA> {

    private static final String ROOT_NAME = "titus.simpleReconciliation.engine.provider";

    private final List<ProviderState<DATA>> providerList;
    private final Map<String, ProviderState<DATA>> providers;

    public ActionProviderSelector(String name,
                                  List<ReconcilerActionProvider<DATA>> providers,
                                  TitusRuntime titusRuntime) {
        this.providerList = providers.stream()
                .map(actionProvider -> new ProviderState<>(name, actionProvider, titusRuntime))
                .collect(Collectors.toList());
        this.providers = providerList.stream().collect(
                Collectors.toMap(p -> p.getActionProvider().getPolicy().getName(), Function.identity())
        );
    }

    public Iterator<ReconcilerActionProvider<DATA>> next(long now) {
        // We handle explicitly the two most common cases for the best performance.
        if (providerList.size() == 1) {
            return handleOne(now);
        }
        if (providerList.size() == 2) {
            return handleTwo(now);
        }
        return handleMany(now);
    }

    public void updateEvaluationTime(String name, long timestamp) {
        ProviderState<DATA> provider = Preconditions.checkNotNull(providers.get(name), "unknown provider: name=%s", name);
        provider.updateEvaluationTime(timestamp);
    }

    private Iterator<ReconcilerActionProvider<DATA>> handleOne(long now) {
        ProviderState<DATA> provider = providerList.get(0);
        if (!provider.isDue(now)) {
            return Collections.emptyIterator();
        }
        return new OneItemIterator<>(provider.getActionProvider());
    }

    private Iterator<ReconcilerActionProvider<DATA>> handleTwo(long now) {
        ProviderState<DATA> provider1 = providerList.get(0);
        ProviderState<DATA> provider2 = providerList.get(1);
        if (provider1.isDue(now)) {
            if (provider2.isDue(now)) {
                int provider1Priority = provider1.getEffectivePriority(now);
                int provider2Priority = provider2.getEffectivePriority(now);
                if (provider1Priority == provider2Priority) {
                    if (provider1.getLastUpdateTimestamp() < provider2.getLastUpdateTimestamp()) {
                        return CollectionsExt.newIterator(provider1.getActionProvider(), provider2.getActionProvider());
                    }
                    return CollectionsExt.newIterator(provider2.getActionProvider(), provider1.getActionProvider());
                } else if (provider1Priority < provider2Priority) {
                    return CollectionsExt.newIterator(provider1.getActionProvider(), provider2.getActionProvider());
                }
                return CollectionsExt.newIterator(provider2.getActionProvider(), provider1.getActionProvider());
            }
            return CollectionsExt.newIterator(provider1.getActionProvider());
        }
        if (provider2.isDue(now)) {
            return CollectionsExt.newIterator(provider2.getActionProvider());
        }
        return Collections.emptyIterator();
    }

    private Iterator<ReconcilerActionProvider<DATA>> handleMany(long now) {
        List<ProviderState<DATA>> filtered = new ArrayList<>();
        for (ProviderState<DATA> p : providerList) {
            if (p.isDue(now)) {
                filtered.add(p);
            }
        }
        filtered.sort(new ProviderStateComparator<>(now));
        List<ReconcilerActionProvider<DATA>> result = new ArrayList<>();
        for (ProviderState<DATA> p : filtered) {
            result.add(p.getActionProvider());
        }
        return result.iterator();
    }

    private static class ProviderState<DATA> {

        private final ReconcilerActionProvider<DATA> actionProvider;
        private final long executionIntervalMs;
        private final long minimumExecutionIntervalMs;
        private final Clock clock;

        private long lastUpdateTimestamp;

        private ProviderState(String name, ReconcilerActionProvider<DATA> actionProvider, TitusRuntime titusRuntime) {
            this.actionProvider = actionProvider;
            this.executionIntervalMs = Math.max(0, actionProvider.getPolicy().getExecutionInterval().toMillis());
            this.minimumExecutionIntervalMs = Math.max(0, actionProvider.getPolicy().getMinimumExecutionInterval().toMillis());
            this.lastUpdateTimestamp = -1;
            this.clock = titusRuntime.getClock();

            Registry registry = titusRuntime.getRegistry();
            PolledMeter.using(registry)
                    .withId(registry.createId(ROOT_NAME + "timeSinceLastEvaluation", "provider", name))
                    .monitorValue(this, self ->
                            clock.wallTime() - self.lastUpdateTimestamp
                    );
        }

        private ReconcilerActionProvider<DATA> getActionProvider() {
            return actionProvider;
        }

        private int getEffectivePriority(long now) {
            int priority = actionProvider.getPolicy().getPriority();
            if (minimumExecutionIntervalMs < 1) {
                return priority;
            }
            long elapsed = now - this.lastUpdateTimestamp;
            if (elapsed < minimumExecutionIntervalMs) {
                return priority;
            }
            return ReconcilerActionProviderPolicy.EXCEEDED_MIN_EXECUTION_INTERVAL_PRIORITY;
        }

        private long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        private boolean isDue(long now) {
            if (executionIntervalMs < 1) {
                return true;
            }
            return lastUpdateTimestamp + executionIntervalMs <= now;
        }

        public void updateEvaluationTime(long now) {
            this.lastUpdateTimestamp = now;
        }
    }

    private static class ProviderStateComparator<DATA> implements Comparator<ProviderState<DATA>> {

        private final long now;

        public ProviderStateComparator(long now) {
            this.now = now;
        }

        @Override
        public int compare(ProviderState<DATA> first, ProviderState<DATA> second) {
            int priority1 = first.getEffectivePriority(now);
            int priority2 = second.getEffectivePriority(now);
            if (priority1 < priority2) {
                return -1;
            }
            if (priority2 < priority1) {
                return 1;
            }
            return Long.compare(first.getLastUpdateTimestamp(), second.getLastUpdateTimestamp());
        }
    }
}
