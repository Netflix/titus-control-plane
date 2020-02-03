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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.time.Clock;

public class ReconcilerExecutorMetrics {

    private static final String ROOT_NAME = "titus.simpleReconciliation.engine.";
    private static final String EVALUATIONS = ROOT_NAME + "evaluations";
    private static final String EXTERNAL_ACTIONS_QUEUE_SIZE = ROOT_NAME + "externalActionQueueSize";
    private static final String SINCE_LAST_EVALUATION = ROOT_NAME + "sinceLastEvaluation";

    private final Id evaluationId;
    private final Id sinceLastEvaluationId;
    private final Id externalActionQueueSizeId;

    private final ConcurrentMap<String, Gauge> externalActionsQueueSizes = new ConcurrentHashMap<>();
    private volatile long lastEvaluationTimestamp;

    private final Registry registry;
    private final Clock clock;

    public ReconcilerExecutorMetrics(String name, TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.clock = titusRuntime.getClock();
        this.lastEvaluationTimestamp = clock.wallTime();

        this.evaluationId = registry.createId(EVALUATIONS, "reconcilerName", name);
        this.sinceLastEvaluationId = registry.createId(SINCE_LAST_EVALUATION, "reconcilerName", name);
        this.externalActionQueueSizeId = registry.createId(EXTERNAL_ACTIONS_QUEUE_SIZE, "reconcilerName", name);

        PolledMeter.using(registry).withId(sinceLastEvaluationId).monitorValue(this, self -> self.clock.wallTime() - self.lastEvaluationTimestamp);
    }

    void shutdown() {
        PolledMeter.remove(registry, sinceLastEvaluationId);
        externalActionsQueueSizes.values().forEach(g -> g.set(0));
        externalActionsQueueSizes.clear();
    }

    void remove(String id) {
        Evaluators.acceptNotNull(externalActionsQueueSizes.remove(id), g -> g.set(0.0));
    }

    void evaluated(long executionTimeNs) {
        registry.timer(evaluationId).record(executionTimeNs, TimeUnit.NANOSECONDS);
        lastEvaluationTimestamp = clock.wallTime();
    }

    void evaluated(long executionTimeNs, Exception error) {
        registry.timer(evaluationId.withTag("error", error.getClass().getSimpleName())).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void updateExternalActionQueueSize(String id, int size) {
        externalActionsQueueSizes.computeIfAbsent(id,
                i -> registry.gauge(externalActionQueueSizeId.withTag("executorId", id))
        ).set(size);
    }
}
