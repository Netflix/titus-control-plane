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

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import rx.Scheduler;

final class AgentStatusSamples {

    private static final AgentInstance AGENT = AgentGenerator.agentInstances().getValue();

    private final String sourceId;
    private final Scheduler scheduler;
    private final AgentStatus ok;

    public AgentStatusSamples(String sourceId, Scheduler scheduler) {
        this.sourceId = sourceId;
        this.scheduler = scheduler;
        this.ok = AgentStatus.healthy(sourceId, AGENT, "Sample", scheduler.now());
    }

    public AgentStatus getOk() {
        return ok;
    }

    public AgentStatus getTerminated() {
        return AgentStatus.terminated(sourceId, AGENT, "Sample", scheduler.now());
    }

    public AgentStatus getBad() {
        return AgentStatus.unhealthy(sourceId, AGENT, "Sample", scheduler.now());
    }
}
