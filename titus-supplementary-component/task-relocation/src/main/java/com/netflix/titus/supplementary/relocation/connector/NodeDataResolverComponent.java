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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
public class NodeDataResolverComponent {

    @Qualifier("fenzo")
    @Bean
    public AgentManagementNodeDataResolver getAgentManagementNodeDataResolver(ReadOnlyAgentOperations agentOperations,
                                                                              AgentDataReplicator agentDataReplicator,
                                                                              RelocationConfiguration relocationConfiguration,
                                                                              KubeApiFacade kubeApiFacade) {
        return new AgentManagementNodeDataResolver(agentOperations,
                agentDataReplicator,
                NodePredicates.getFenzoNodePredicate(kubeApiFacade.getNodeInformer()),
                relocationConfiguration, kubeApiFacade);
    }

    @Qualifier("kubeScheduler")
    @Bean
    public KubernetesNodeDataResolver getKubernetesNodeDataResolver(RelocationConfiguration configuration,
                                                                    KubeApiFacade kubeApiFacade) {
        return new KubernetesNodeDataResolver(configuration,
                kubeApiFacade,
                NodePredicates.getKubeSchedulerNodePredicate()
        );
    }

    @Primary
    @Bean
    public NodeDataResolver getNodeDataResolver(@Qualifier("fenzo") AgentManagementNodeDataResolver agentManagementNodeDataResolver,
                                                @Qualifier("kubeScheduler") KubernetesNodeDataResolver kubernetesNodeDataResolver) {
        return new AggregatingNodeDataResolver(Arrays.asList(agentManagementNodeDataResolver, kubernetesNodeDataResolver));
    }
}
