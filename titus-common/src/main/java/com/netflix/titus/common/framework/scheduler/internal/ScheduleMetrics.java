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

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.SpectatorExt.FsmMetrics;
import com.netflix.titus.common.util.time.Clock;

/**
 * Single schedule metrics.
 */
class ScheduleMetrics {

    static final String ROOT_NAME = "titus.localScheduler.";

    private final Clock clock;
    private final Registry registry;

    private final Counter successes;
    private final Counter failures;

    private final Id waitingId;
    private final Id runningId;
    private final Id cancellingId;

    private FsmMetrics<SchedulingState> currentState;
    private Schedule lastSchedule;

    ScheduleMetrics(Schedule schedule, Clock clock, Registry registry) {
        this.lastSchedule = schedule;
        this.clock = clock;
        this.registry = registry;

        this.currentState = SpectatorExt.fsmMetrics(
                registry.createId(ROOT_NAME + "scheduleState", "name", schedule.getDescriptor().getName()),
                SchedulingState::isFinal,
                SchedulingState.Waiting,
                registry
        );
        this.successes = registry.counter(ROOT_NAME + "executions",
                "name", schedule.getDescriptor().getName(),
                "status", "succeeded"
        );
        this.failures = registry.counter(ROOT_NAME + "executions",
                "name", schedule.getDescriptor().getName(),
                "status", "failed"
        );

        this.waitingId = registry.createId(ROOT_NAME + "waitingTimeMs", "name", schedule.getDescriptor().getName());
        PolledMeter.using(registry)
                .withId(waitingId)
                .monitorValue(this, self -> howLongInState(SchedulingState.Waiting));
        this.runningId = registry.createId(ROOT_NAME + "runningTimeMs", "name", schedule.getDescriptor().getName());
        PolledMeter.using(registry)
                .withId(runningId)
                .monitorValue(this, self -> howLongInState(SchedulingState.Running));
        this.cancellingId = registry.createId(ROOT_NAME + "cancellingTimeMs", "name", schedule.getDescriptor().getName());
        PolledMeter.using(registry)
                .withId(cancellingId)
                .monitorValue(this, self -> howLongInState(SchedulingState.Cancelling));
    }

    void onNewScheduledActionExecutor(Schedule schedule) {
        this.lastSchedule = schedule;
        currentState.transition(SchedulingState.Waiting);
    }

    void onSchedulingStateUpdate(Schedule schedule) {
        this.lastSchedule = schedule;
        SchedulingState state = schedule.getCurrentAction().getStatus().getState();
        if (state.isFinal()) {
            if (state == SchedulingState.Succeeded) {
                successes.increment();
            } else {
                failures.increment();
            }
        } else {
            currentState.transition(state);
        }
    }

    void onScheduleRemoved(Schedule schedule) {
        this.lastSchedule = schedule;
        currentState.transition(SchedulingState.Failed);

        PolledMeter.remove(registry, waitingId);
        PolledMeter.remove(registry, runningId);
        PolledMeter.remove(registry, cancellingId);
    }

    private long howLongInState(SchedulingState expectedState) {
        SchedulingState state = lastSchedule.getCurrentAction().getStatus().getState();
        if (state != expectedState) {
            return 0;
        }
        return clock.wallTime() - lastSchedule.getCurrentAction().getStatus().getTimestamp();
    }
}
