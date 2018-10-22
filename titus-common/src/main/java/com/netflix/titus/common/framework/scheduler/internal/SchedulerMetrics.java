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

package com.netflix.titus.common.framework.scheduler.internal;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.util.time.Clock;

class SchedulerMetrics {

    private final DefaultLocalScheduler scheduler;
    private final Clock clock;
    private final Registry registry;

    private final Id activeSchedulesId;
    private final Id archivedSchedulesId;
    private final Id lastEvaluationId;
    private final Timer evaluationTimer;

    private long lastEvaluationTime;

    SchedulerMetrics(DefaultLocalScheduler scheduler, Clock clock, Registry registry) {
        this.clock = clock;
        this.registry = registry;
        this.scheduler = scheduler;
        this.lastEvaluationTime = clock.wallTime();

        this.activeSchedulesId = registry.createId(ScheduleMetrics.ROOT_NAME + "active");
        PolledMeter.using(registry)
                .withId(activeSchedulesId)
                .monitorValue(this, self -> self.scheduler.getActiveSchedules().size());
        this.archivedSchedulesId = registry.createId(ScheduleMetrics.ROOT_NAME + "archived");
        PolledMeter.using(registry)
                .withId(activeSchedulesId)
                .monitorValue(this, self -> self.scheduler.getArchivedSchedules().size());

        this.evaluationTimer = registry.timer(ScheduleMetrics.ROOT_NAME + "evaluationTime");
        this.lastEvaluationId = registry.createId(ScheduleMetrics.ROOT_NAME + "lastEvaluationMs");
        PolledMeter.using(registry)
                .withId(lastEvaluationId)
                .monitorValue(this, self -> self.clock.wallTime() - self.lastEvaluationTime);
    }

    void shutdown() {
        PolledMeter.remove(registry, activeSchedulesId);
        PolledMeter.remove(registry, archivedSchedulesId);
        PolledMeter.remove(registry, lastEvaluationId);
    }

    void recordEvaluationTime(long evaluationTimeMs) {
        this.lastEvaluationTime = clock.wallTime();
        evaluationTimer.record(evaluationTimeMs, TimeUnit.MILLISECONDS);
    }
}
