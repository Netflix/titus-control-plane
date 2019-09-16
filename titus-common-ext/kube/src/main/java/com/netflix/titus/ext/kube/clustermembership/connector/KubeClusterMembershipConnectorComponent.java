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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class KubeClusterMembershipConnectorComponent {

    public static final String LOCAL_MEMBER_INITIAL = "localMemberInitial";

    @Bean
    public KubeConnectorConfiguration getK8ConnectorConfiguration(Environment environment) {
        return new KubeConnectorConfigurationBean(environment, KubeConnectorConfiguration.PREFIX);
    }

    @Bean
    public ApiClient getApiClient(KubeConnectorConfiguration configuration) {
        String k8ApiServerUri = Preconditions.checkNotNull(
                StringExt.safeTrim(configuration.getK8ApiServerUri()),
                "Kubernetes address not set"
        );

        ApiClient client;
        try {
            client = ClientBuilder
                    .standard()
                    .setBasePath(k8ApiServerUri)
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
        return client;
    }

    @Bean
    public ClusterMembershipConnector getClusterMembershipConnector(
            KubeConnectorConfiguration configuration,
            @Qualifier(LOCAL_MEMBER_INITIAL) ClusterMember initial,
            ApiClient k8ApiClient,
            TitusRuntime titusRuntime) {
        return new KubeClusterMembershipConnector(
                initial,
                new DefaultKubeMembershipExecutor(k8ApiClient, configuration.getNamespace()),
                new DefaultKubeLeaderElectionExecutor(
                        k8ApiClient,
                        configuration.getNamespace(),
                        configuration.getClusterName(),
                        Duration.ofMillis(configuration.getLeaseDurationMs()),
                        initial.getMemberId(),
                        titusRuntime
                ),
                configuration,
                titusRuntime
        );
    }
}
