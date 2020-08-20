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

package com.netflix.titus.ext.kube.clustermembership.connector;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.StringExt;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * @deprecated {@link ApiClient} should be provided independently of the cluster membership component.
 */
@Component
public class KubeClientComponent {

    private static final Logger logger = LoggerFactory.getLogger(KubeClientComponent.class);

    @Bean
    public ApiClient getApiClient(KubeClusterMembershipConfiguration configuration) {
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
}
