/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.std;

import com.netflix.titus.runtime.connector.kubernetes.KubeApiException;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Mono;

/**
 * {@link StdKubeApiFacade} encapsulates Kube Java, except the entity model and the informer API. The latter is
 * provided as a set of interfaces (unlike ApiClient or CoreV1Api), so it is easy to mock in the test code.

 * @deprecated Use Fabric8IO instead.
 */
public interface StdKubeApiFacade {

    // Nodes

    void deleteNode(String nodeName) throws KubeApiException;

    SharedIndexInformer<V1Node> getNodeInformer();

    // Pods

    void createNamespacedPod(String namespace, V1Pod pod) throws KubeApiException;

    Mono<V1Pod> createNamespacedPodAsync(String namespace, V1Pod pod);

    void deleteNamespacedPod(String namespace, String nodeName) throws KubeApiException;

    void deleteNamespacedPod(String namespace, String podName, int deleteGracePeriod) throws KubeApiException;

    SharedIndexInformer<V1Pod> getPodInformer();

    // Persistent volumes

    void createPersistentVolume(V1PersistentVolume v1PersistentVolume) throws KubeApiException;

    void createNamespacedPersistentVolumeClaim(String namespace, V1PersistentVolumeClaim v1PersistentVolumeClaim) throws KubeApiException;

    void deleteNamespacedPersistentVolumeClaim(String namespace, String volumeClaimName) throws KubeApiException;

    void replacePersistentVolume(V1PersistentVolume persistentVolume) throws KubeApiException;

    void deletePersistentVolume(String volumeName) throws KubeApiException;

    SharedIndexInformer<V1PersistentVolume> getPersistentVolumeInformer();

    SharedIndexInformer<V1PersistentVolumeClaim> getPersistentVolumeClaimInformer();

    /**
     * Provide information about how up to date the pod informer data is. If the pod informer is connected, and synced
     * the staleness is 0.
     *
     * @return -1 if the pod informer was never synced or the data staleness time
     */
    default long getPodInformerStaleness() {
        return 0;
    }

    /**
     * Returns true, if the Kubernetes integration subsystem is ready for scheduling.
     */
    default boolean isReadyForScheduling() {
        return true;
    }
}
