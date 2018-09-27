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

package com.netflix.titus.master.agent.endpoint.grpc;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.HealthState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.InstanceOverrideState;

import static com.netflix.titus.common.util.StringExt.parseEnumIgnoreCase;
import static com.netflix.titus.common.util.StringExt.splitByComma;

final class AgentQueryExecutor {

    // list of agent instance ids
    static final String KEY_AGENT_IDS = "instanceIds";

    // list of instance group ids
    static final String KEY_INSTANCE_GROUP_IDS = "instanceGroupIds";

    // agent deployment state
    static final String KEY_DEPLOYMENT_STATE = "deploymentState";

    // override state of an agent
    static final String KEY_OVERRIDE_STATE = "overrideState";

    // current agent health state
    static final String KEY_HEALTH_STATE = "healthState";

    static class AgentQueryEvaluators {
        private final Set<String> agentIds;
        private final Set<String> instanceGroupIds;
        private final Optional<InstanceLifecycleState> deploymentState;
        private final Optional<InstanceOverrideState> overrideState;
        private final Optional<HealthState> healthState;

        public AgentQueryEvaluators(AgentQuery query) {
            Map<String, String> criteria = query.getFilteringCriteriaMap();

            this.agentIds = new HashSet<>(splitByComma(criteria.get(KEY_AGENT_IDS)));
            this.instanceGroupIds = new HashSet<>(splitByComma(criteria.get(KEY_INSTANCE_GROUP_IDS)));

            this.deploymentState = Optional.ofNullable(criteria.get(KEY_DEPLOYMENT_STATE))
                    .map(state -> parseEnumIgnoreCase(state, InstanceLifecycleState.class));

            this.overrideState = Optional.ofNullable(criteria.get(KEY_OVERRIDE_STATE))
                    .map(state -> parseEnumIgnoreCase(state, InstanceOverrideState.class));

            this.healthState = Optional.ofNullable(criteria.get(KEY_HEALTH_STATE))
                    .map(state -> parseEnumIgnoreCase(state, HealthState.class));
        }

        boolean matchesAgentId(AgentInstance agentInstance) {
            return agentIds.isEmpty() || agentIds.contains(agentInstance.getId());
        }

        boolean matchesInstanceGroupId(AgentInstanceGroup agentInstanceGroup) {
            return instanceGroupIds.isEmpty() || instanceGroupIds.contains(agentInstanceGroup.getId());
        }

        boolean matchesDeploymentState(AgentInstance agentInstance) {
            return deploymentState.map(deploymentState -> deploymentState.equals(agentInstance.getLifecycleStatus().getState())).orElse(true);
        }

//        boolean matchesHealthState(AgentInstance agentInstance) {
//            return healthState.map(healthState -> healthState.equals(agentInstance.getHealthStatus().getState())).orElse(true);
//        }
    }

    static List<AgentInstance> findAgentInstances(AgentQuery query, AgentManagementService agentManagementService) {
        AgentQueryEvaluators queryEvaluator = new AgentQueryEvaluators(query);
        Predicate<Pair<AgentInstanceGroup, AgentInstance>> predicate = pair -> {
            AgentInstanceGroup instanceGroup = pair.getLeft();
            AgentInstance agentInstance = pair.getRight();
            return queryEvaluator.matchesAgentId(agentInstance)
                    && queryEvaluator.matchesInstanceGroupId(instanceGroup)
                    && queryEvaluator.matchesDeploymentState(agentInstance);
        };
        return agentManagementService.findAgentInstances(predicate).stream()
                .flatMap(pair -> pair.getRight().stream())
                .collect(Collectors.toList());
    }
}
