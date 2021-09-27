/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.main;

import java.time.Duration;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeClusterMembershipConfiguration;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeClusterMembershipConnector;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeLeaderElectionExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeMembershipExecutor;
import io.kubernetes.client.openapi.ApiClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "titus.ext.kube.enabled", havingValue = "true", matchIfMissing = true)
public class MainKubeClusterMembershipConnectorComponent {

    public static final String LOCAL_MEMBER_INITIAL = "localMemberInitial";

    @Bean
    public KubeClusterMembershipConfiguration getKubeClusterMembershipConfiguration(MyEnvironment environment) {
        return Archaius2Ext.newConfiguration(KubeClusterMembershipConfiguration.class, KubeClusterMembershipConfiguration.PREFIX, environment);
    }

    @Bean
    public KubeMembershipExecutor getKubeMembershipExecutor(KubeClusterMembershipConfiguration configuration, ApiClient kubeApiClient) {
        return new MainKubeMembershipExecutor(kubeApiClient, configuration.getNamespace());
    }

    @Bean
    public KubeLeaderElectionExecutor getDefaultKubeLeaderElectionExecutor(KubeClusterMembershipConfiguration configuration,
                                                                           @Qualifier(LOCAL_MEMBER_INITIAL) ClusterMember initial,
                                                                           ApiClient kubeApiClient,
                                                                           TitusRuntime titusRuntime) {
        return new MainKubeLeaderElectionExecutor(
                kubeApiClient,
                configuration.getNamespace(),
                configuration.getClusterName(),
                Duration.ofMillis(configuration.getLeaseDurationMs()),
                initial.getMemberId(),
                titusRuntime
        );
    }

    @Bean
    public ClusterMembershipConnector getClusterMembershipConnector(
            KubeClusterMembershipConfiguration configuration,
            @Qualifier(LOCAL_MEMBER_INITIAL) ClusterMember initial,
            KubeMembershipExecutor membershipExecutor,
            KubeLeaderElectionExecutor leaderElectionExecutor,
            TitusRuntime titusRuntime) {
        return new KubeClusterMembershipConnector(
                initial,
                membershipExecutor,
                leaderElectionExecutor,
                configuration,
                titusRuntime
        );
    }
}
