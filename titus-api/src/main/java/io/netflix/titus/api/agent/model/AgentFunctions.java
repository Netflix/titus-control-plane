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

package io.netflix.titus.api.agent.model;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.agent.service.AgentManagementService;

/**
 * Collection of functions for agent entity transformations.
 */
public final class AgentFunctions {

    private AgentFunctions() {
    }

    public static Set<String> instanceGroupIds(Collection<AgentInstanceGroup> instanceGroups) {
        return instanceGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toSet());
    }

    public static Set<String> instanceIds(Collection<AgentInstance> instances) {
        return instances.stream().map(AgentInstance::getId).collect(Collectors.toSet());
    }

    public static Optional<AgentInstanceGroup> getInstanceGroup(String id, AgentManagementService agentManagementService) {
        try {
            return Optional.of(agentManagementService.getInstanceGroup(id));
        } catch (AgentManagementException e) {
            return Optional.empty();
        }
    }
}
