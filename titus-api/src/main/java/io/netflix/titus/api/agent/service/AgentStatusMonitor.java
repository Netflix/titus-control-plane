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

package io.netflix.titus.api.agent.service;

import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import rx.Observable;

/**
 * {@link AgentStatusMonitor} provides information about perceived agent health status. This information
 * may come from many different sources.
 */
public interface AgentStatusMonitor {

    /**
     * Return current health status for an agent with the given id.
     *
     * @throws AgentManagementException {@link AgentManagementException.ErrorCode#AgentNotFound} if the agent instance is not found
     */
    AgentStatus getStatus(String agentInstanceId);

    /**
     * Observable that emits {@link AgentStatus}es.
     */
    Observable<AgentStatus> monitor();
}
