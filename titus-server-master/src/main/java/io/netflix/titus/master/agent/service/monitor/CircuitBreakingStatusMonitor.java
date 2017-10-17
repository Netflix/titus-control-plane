/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.agent.service.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import io.netflix.titus.api.agent.model.monitor.AgentStatusExt;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

/**
 * Given a total number of agents, sets an upper bound on how many agents can be set in 'disabled' state.
 */
public class CircuitBreakingStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakingStatusMonitor.class);

    private final AgentStatusMonitor delegate;
    private final Supplier<Integer> disablePercentageThreshold;
    private final Supplier<Integer> agentCountProvider;
    private final Scheduler scheduler;

    public CircuitBreakingStatusMonitor(AgentStatusMonitor delegate,
                                        Supplier<Integer> disablePercentageThreshold,
                                        Supplier<Integer> agentCountProvider,
                                        Scheduler scheduler) {
        this.delegate = delegate;
        this.disablePercentageThreshold = disablePercentageThreshold;
        this.agentCountProvider = agentCountProvider;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return delegate.monitor()
                .compose(ObservableExt.combine(StateCache::new))
                .flatMap(pair -> {
                    AgentStatus status = pair.getLeft();
                    StateCache cache = pair.getRight();

                    if (status.getStatusCode() == AgentStatusCode.Healthy) {
                        return handleOkStatus(status, cache);
                    }
                    return handleBadStatus(status, cache);
                });
    }

    private Observable<AgentStatus> handleOkStatus(AgentStatus status, StateCache cache) {
        cache.remove(status.getInstance().getId());
        return Observable.just(status);
    }

    private Observable<AgentStatus> handleBadStatus(AgentStatus status, StateCache cache) {
        cache.removeExpired();
        cache.add(status);

        int disableLimit = allowedToDisable();
        if (disableLimit > cache.getDisabledSize()) {
            cache.addDisabled(status);
            return Observable.just(status);
        } else {
            logger.info("Agent {} is in unhealthy state but circuit breaker is opened: disableLimit={} < badAgents={}",
                    status.getInstance().getId(), disableLimit, cache.getAllSize());
        }

        return Observable.empty();
    }

    private int allowedToDisable() {
        int thresholdPerc = disablePercentageThreshold.get();
        int agentCount = agentCountProvider.get();
        return (agentCount * thresholdPerc) / 100;
    }

    private class StateCache {
        private final Map<String, AgentStatus> badAgents = new HashMap<>();
        private final Map<String, AgentStatus> allAgents = new HashMap<>();

        void add(AgentStatus status) {
            allAgents.put(status.getInstance().getId(), status);
        }

        void addDisabled(AgentStatus status) {
            badAgents.put(status.getInstance().getId(), status);
        }

        void remove(String agentId) {
            badAgents.remove(agentId);
            allAgents.remove(agentId);
        }

        void removeExpired() {
            badAgents.values().removeIf(status -> AgentStatusExt.isExpired(status, scheduler));
            allAgents.values().removeIf(status -> AgentStatusExt.isExpired(status, scheduler));
        }

        int getAllSize() {
            return allAgents.size();
        }

        int getDisabledSize() {
            return badAgents.size();
        }
    }
}
