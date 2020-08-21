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

import java.util.Arrays;

import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class NodeDataResolverComponent {

    @Bean
    public AgentManagementNodeDataResolver getAgentManagementNodeDataResolver(ReadOnlyAgentOperations agentOperations,
                                                                              AgentDataReplicator agentDataReplicator,
                                                                              KubeApiFacade kubeApiFacade) {
        return new AgentManagementNodeDataResolver(agentOperations,
                agentDataReplicator,
                NodePredicates.getFenzoNodePredicate(kubeApiFacade.getNodeInformer())
        );
    }

    @Bean
    public KubernetesNodeDataResolver getKubernetesNodeDataResolver(RelocationConfiguration configuration,
                                                                    KubeApiFacade kubeApiFacade) {
        return new KubernetesNodeDataResolver(configuration,
                kubeApiFacade,
                NodePredicates.getKubeSchedulerNodePredicate()
        );
    }

    @Bean
    public NodeDataResolver getNodeDataResolver(AgentManagementNodeDataResolver agentManagementNodeDataResolver,
                                                KubernetesNodeDataResolver kubernetesNodeDataResolver) {
        return new AggregatingNodeDataResolver(Arrays.asList(agentManagementNodeDataResolver, kubernetesNodeDataResolver));
    }
}
