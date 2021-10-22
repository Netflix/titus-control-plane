package com.netflix.titus.gateway.kubernetes;

import com.netflix.titus.runtime.connector.kubernetes.Fabric8IOClients;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

public interface KubeApiClient {
    public NamespacedKubernetesClient getApiClient();
}
