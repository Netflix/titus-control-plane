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

package com.netflix.titus.runtime.connector.kubernetes;

import java.util.Optional;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

public final class Fabric8IOClients {

    private Fabric8IOClients() {
    }

    /**
     * TODO Add metrics
     */
    public static NamespacedKubernetesClient createFabric8IOClient() {
        return new DefaultKubernetesClient();
    }

    public static Optional<Throwable> checkKubeConnectivity(NamespacedKubernetesClient fabric8IOClient) {
        try {
            fabric8IOClient.apiServices().list();
        } catch (Throwable e) {
            return Optional.of(e);
        }
        return Optional.empty();
    }

    public static NamespacedKubernetesClient mustHaveKubeConnectivity(NamespacedKubernetesClient fabric8IOClient) {
        checkKubeConnectivity(fabric8IOClient).ifPresent(error -> {
            throw new IllegalStateException("Kube client connectivity error", error);
        });
        return fabric8IOClient;
    }
}
