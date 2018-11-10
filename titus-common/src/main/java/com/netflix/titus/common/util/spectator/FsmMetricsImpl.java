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

package com.netflix.titus.common.util.spectator;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.util.StringExt;

/**
 * Metrics collector for a Finite State Machine (FSM). It reports the current state of an FSM, and the state transitions.
 * If the FSM reaches the terminal state, the current state indicators are cleared to prevent infinite accumulation.
 * If multiple state transitions happen simultaneously, the result state is undefined. Callers are expected to serialize
 * all updates.
 */
class FsmMetricsImpl<S> implements SpectatorExt.FsmMetrics<S> {

    private final Function<S, String> nameOf;
    private final Function<S, Boolean> finalStateEval;
    private final Registry registry;
    private final Id baseStateId;
    private final Id baseUpdatesId;

    private final AtomicReference<StateHolder> currentState;

    FsmMetricsImpl(Id rootId,
                   Function<S, String> nameOf,
                   Function<S, Boolean> finalStateEval,
                   S initialState,
                   Registry registry) {
        this.nameOf = nameOf;
        this.finalStateEval = finalStateEval;
        this.registry = registry;

        String effectiveRootName = rootId.name().endsWith(".") ? rootId.name() : rootId.name() + '.';
        this.baseStateId = registry.createId(effectiveRootName + "currentState", rootId.tags());
        this.baseUpdatesId = registry.createId(effectiveRootName + "updates", rootId.tags());
        this.currentState = new AtomicReference<>(new StateHolder(initialState, ""));
    }

    @Override
    public void transition(S nextState) {
        transition(nextState, "");
    }

    @Override
    public void transition(S nextState, String reason) {
        boolean isCurrentFinal = finalStateEval.apply(currentState.get().state);
        if (isCurrentFinal || currentState.get().state == nextState) {
            return;
        }
        currentState.getAndSet(new StateHolder(nextState, reason)).leaveState();
    }

    enum StateHolderLifecycle {Active, Inactive, Removable}

    private class StateHolder {

        private final Registry registry;
        private final S state;
        private final Id currentStateId;
        private volatile StateHolderLifecycle lifecycle;

        private StateHolder(S state, String reason) {
            String stateName = nameOf.apply(state);
            this.state = state;
            this.lifecycle = StateHolderLifecycle.Active;
            this.registry = FsmMetricsImpl.this.registry;

            Id currentUpdateId = baseUpdatesId.withTag("state", stateName);
            if (StringExt.isNotEmpty(reason)) {
                currentUpdateId = currentUpdateId.withTag("reason", reason);
            }
            registry.counter(currentUpdateId).increment();

            this.currentStateId = baseStateId.withTag("state", stateName);
            if (!finalStateEval.apply(state)) {
                PolledMeter.using(registry).withId(this.currentStateId).monitorValue(this,
                        // Be sure to access all fields via 'self' object in this method
                        self -> {
                            switch (self.lifecycle) {
                                case Active:
                                    return 1;
                                case Inactive:
                                    self.lifecycle = StateHolderLifecycle.Removable;
                                    return 0;
                            }
                            // Removable
                            PolledMeter.remove(self.registry, self.currentStateId);
                            return 0;
                        }
                );
            }
        }

        private void leaveState() {
            this.lifecycle = StateHolderLifecycle.Inactive;
        }
    }
}
