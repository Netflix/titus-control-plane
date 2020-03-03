/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.kube.clustermembership.connector;

import java.io.IOException;
import java.time.Duration;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@ConditionalOnProperty(name = "titus.ext.kube.enabled", havingValue = "true", matchIfMissing = true)
public class KubeClusterMembershipConnectorComponent {

    private static final Logger logger = LoggerFactory.getLogger(KubeClusterMembershipConnectorComponent.class);

    public static final String LOCAL_MEMBER_INITIAL = "localMemberInitial";

    @Bean
    public KubeConnectorConfiguration getKubeConnectorConfiguration(Environment environment) {
        return new KubeConnectorConfigurationBean(environment, KubeConnectorConfiguration.PREFIX);
    }

    @Bean
    public ApiClient getApiClient(KubeConnectorConfiguration configuration) {
        String kubeApiServerUri = StringExt.safeTrim(configuration.getKubeApiServerUri());
        String kubeConfigPath = StringExt.safeTrim(configuration.getKubeConfigPath());

        Preconditions.checkState(!kubeApiServerUri.isEmpty() || !kubeConfigPath.isEmpty(),
                "Kubernetes address not set"
        );

        ApiClient client;
        if (kubeApiServerUri.isEmpty()) {
            try {
                logger.info("Initializing Kube ApiClient from config file: {}", kubeConfigPath);
                client = Config.fromConfig(kubeConfigPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            logger.info("Initializing Kube ApiClient with URI: {}", kubeApiServerUri);
            client = Config.fromUrl(kubeApiServerUri);
        }

        client.setReadTimeout(0); // infinite timeout
        return client;
    }

    @Bean
    public KubeMembershipExecutor getKubeMembershipExecutor(KubeConnectorConfiguration configuration, ApiClient kubeApiClient) {
        return new DefaultKubeMembershipExecutor(kubeApiClient, configuration.getNamespace());
    }

    @Bean
    public KubeLeaderElectionExecutor getDefaultKubeLeaderElectionExecutor(KubeConnectorConfiguration configuration,
                                                                           @Qualifier(LOCAL_MEMBER_INITIAL) ClusterMember initial,
                                                                           ApiClient kubeApiClient,
                                                                           TitusRuntime titusRuntime) {
        return new DefaultKubeLeaderElectionExecutor(
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
            KubeConnectorConfiguration configuration,
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
