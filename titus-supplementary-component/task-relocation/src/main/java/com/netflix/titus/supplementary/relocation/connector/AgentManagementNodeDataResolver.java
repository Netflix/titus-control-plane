/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.RelocationAttributes;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.models.V1Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentManagementNodeDataResolver implements NodeDataResolver {
    private static final Logger logger = LoggerFactory.getLogger(AgentManagementNodeDataResolver.class);
    private final ReadOnlyAgentOperations agentOperations;
    private final AgentDataReplicator agentDataReplicator;
    private final Predicate<AgentInstance> fenzoNodeFilter;
    private final RelocationConfiguration relocationConfiguration;
    private final Indexer<V1Node> k8sNodeIndexer;
    private final Function<String, Matcher> badConditionMatcherFactory;

    public AgentManagementNodeDataResolver(ReadOnlyAgentOperations agentOperations,
                                           AgentDataReplicator agentDataReplicator,
                                           Predicate<AgentInstance> fenzoNodeFilter,
                                           RelocationConfiguration relocationConfiguration,
                                           KubeApiFacade kubeApiFacade) {
        this.agentOperations = agentOperations;
        this.agentDataReplicator = agentDataReplicator;
        this.fenzoNodeFilter = fenzoNodeFilter;
        this.relocationConfiguration = relocationConfiguration;
        k8sNodeIndexer = kubeApiFacade.getNodeInformer().getIndexer();
        this.badConditionMatcherFactory = RegExpExt.dynamicMatcher(relocationConfiguration::getBadNodeConditionPattern,
                "titus.relocation.badNodeConditionPattern", Pattern.DOTALL, logger);
    }

    @Override
    public Map<String, Node> resolve() {
        List<Pair<AgentInstanceGroup, List<AgentInstance>>> all = agentOperations.findAgentInstances(pair ->
                fenzoNodeFilter.test(pair.getRight())
        );
        Map<String, Node> result = new HashMap<>();
        all.forEach(pair -> {
            AgentInstanceGroup serverGroup = pair.getLeft();
            List<AgentInstance> instances = pair.getRight();
            instances.forEach(instance -> result.put(instance.getId(), toNode(serverGroup, instance)));
        });
        return result;
    }

    @Override
    public long getStalenessMs() {
        return agentDataReplicator.getStalenessMs();
    }

    private Node toNode(AgentInstanceGroup serverGroup, AgentInstance instance) {
        boolean relocationRequired = Boolean.parseBoolean(
                instance.getAttributes().get(RelocationAttributes.RELOCATION_REQUIRED)
        );
        boolean relocationRequiredImmediately = Boolean.parseBoolean(
                instance.getAttributes().get(RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY)
        );
        boolean relocationNotAllowed = Boolean.parseBoolean(
                instance.getAttributes().get(RelocationAttributes.RELOCATION_NOT_ALLOWED)
        );

        boolean serverGroupRelocationRequired = serverGroup.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Removable;
        boolean isNodeConditionBad = false;
        V1Node k8sNode = k8sNodeIndexer.getByKey(instance.getId());
        if (k8sNode != null) {
            isNodeConditionBad = NodePredicates.hasBadCondition(k8sNode, badConditionMatcherFactory,
                    relocationConfiguration.getNodeConditionTransitionTimeThresholdSeconds());
        }

        return Node.newBuilder()
                .withId(instance.getId())
                .withServerGroupId(serverGroup.getId())
                .withRelocationRequired(relocationRequired)
                .withRelocationRequiredImmediately(relocationRequiredImmediately)
                .withRelocationNotAllowed(relocationNotAllowed)
                .withServerGroupRelocationRequired(serverGroupRelocationRequired)
                .withBadCondition(isNodeConditionBad)
                .build();
    }
}
