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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.fabric8io;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.junit.rules.ExternalResource;

public class Fabric8IOKubeExternalResource extends ExternalResource {

    private NamespacedKubernetesClient client;

    @Override
    protected void before() {
        String kubeServer = System.getenv("KUBE_API_SERVER_URL");
        if (kubeServer == null) {
            this.client = new DefaultKubernetesClient();
        } else {
            Config config = new ConfigBuilder()
                    .withMasterUrl(kubeServer)
                    .build();
            this.client = new DefaultKubernetesClient(config);
        }
    }

    @Override
    protected void after() {
        if (client != null) {
            client.close();
        }
    }

    public NamespacedKubernetesClient getClient() {
        return client;
    }
}
