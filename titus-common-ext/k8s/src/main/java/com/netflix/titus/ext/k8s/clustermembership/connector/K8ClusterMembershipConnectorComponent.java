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

package com.netflix.titus.ext.k8s.clustermembership.connector;

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
public class K8ClusterMembershipConnectorComponent {

    public static final String LOCAL_MEMBER_INITIAL = "localMemberInitial";

    @Bean
    public K8ConnectorConfiguration getK8ConnectorConfiguration(Environment environment) {
        return new K8ConnectorConfigurationBean(environment, K8ConnectorConfiguration.PREFIX);
    }

    @Bean
    public ApiClient getApiClient(K8ConnectorConfiguration configuration) {
        String k8ApiServerUri = Preconditions.checkNotNull(
                StringExt.safeTrim(configuration.getK8ApiServerUri()),
                "K8S address not set"
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
            K8ConnectorConfiguration configuration,
            @Qualifier(LOCAL_MEMBER_INITIAL) ClusterMember initial,
            ApiClient k8ApiClient,
            TitusRuntime titusRuntime) {
        return new K8ClusterMembershipConnector(
                initial,
                new DefaultK8MembershipExecutor(k8ApiClient, configuration.getNamespace()),
                new DefaultK8LeaderElectionExecutor(
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
