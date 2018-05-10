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

package com.netflix.titus.master.agent.service.monitor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.common.util.rx.ObservableExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;

/**
 * Supplementary {@link AgentStatusMonitor} implementation that enables/disables signal from the underlying
 * implementation.
 */
public class OnOffStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(OnOffStatusMonitor.class);

    static final long CHECK_INTERVAL_MS = 1_000;

    private static final String SOURCE_ID = "onOffStatusMonitor";

    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor delegate;
    private final Supplier<Boolean> isOn;
    private final String offMessage;
    private final Scheduler scheduler;

    public OnOffStatusMonitor(AgentManagementService agentManagementService,
                              AgentStatusMonitor delegate,
                              Supplier<Boolean> isOn,
                              Scheduler scheduler) {
        this.agentManagementService = agentManagementService;
        this.delegate = delegate;
        this.isOn = isOn;
        this.offMessage = "Disabled downstream status monitor " + delegate.getClass().getSimpleName();
        this.scheduler = scheduler;
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        if (isOn.get()) {
            return delegate.getStatus(agentInstanceId);
        }
        return AgentStatus.healthy(SOURCE_ID, agentManagementService.getAgentInstance(agentInstanceId), offMessage, scheduler.now());
    }

    @Override
    public boolean isHealthy(String agentInstanceId) {
        return !isOn.get() || delegate.isHealthy(agentInstanceId);
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return Observable.merge(delegate.monitor(), Observable.interval(CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler))
                .compose(ObservableExt.combine(() -> new StateCache(isOn.get())))
                .flatMap(pair -> {
                            StateCache stateCache = pair.getRight();
                            if (pair.getLeft() instanceof AgentStatus) {
                                return handleStatus(stateCache, (AgentStatus) pair.getLeft());
                            }
                            boolean newIsOn = isOn.get();
                            if (stateCache.isOn() == newIsOn) {
                                return Observable.empty();
                            }
                            return newIsOn ? doTurnOn(stateCache) : doTurnOff(stateCache);
                        }

                );
    }

    private Observable<AgentStatus> doTurnOn(StateCache stateCache) {
        logger.info("Re-enabling agent status monitor {}", delegate.getClass().getSimpleName());
        stateCache.setOn(true);
        return Observable.from(stateCache.getAll());
    }

    private Observable<AgentStatus> doTurnOff(StateCache stateCache) {
        logger.info("Disabling agent status monitor {}", delegate.getClass().getSimpleName());
        stateCache.setOn(false);
        List<AgentStatus> good = stateCache.getAll().stream()
                .map(status -> {
                    if (status.getStatusCode() == AgentStatusCode.Healthy) {
                        return status;
                    }
                    return AgentStatus.healthy(status.getSourceId(), status.getAgentInstance(), offMessage, status.getEmitTime(), status.getComponents());
                }).collect(Collectors.toList());
        return Observable.from(good);
    }

    private Observable<AgentStatus> handleStatus(StateCache stateCache, AgentStatus status) {
        if (status.getStatusCode() == AgentStatusCode.Healthy) {
            stateCache.remove(status.getAgentInstance().getId());
        } else {
            stateCache.replace(status);
        }
        return stateCache.isOn() ? Observable.just(status) : Observable.empty();
    }

    private static class StateCache {
        private boolean on;
        private final Map<String, AgentStatus> badAgents = new HashMap<>();

        StateCache(boolean on) {
            this.on = on;
        }

        boolean isOn() {
            return on;
        }

        void setOn(boolean on) {
            this.on = on;
        }

        Collection<AgentStatus> getAll() {
            return badAgents.values();
        }

        void remove(String agentId) {
            badAgents.remove(agentId);
        }

        void replace(AgentStatus status) {
            badAgents.put(status.getAgentInstance().getId(), status);
        }
    }
}
