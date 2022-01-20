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

import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import com.netflix.titus.runtime.connector.kubernetes.v1.V1OpportunisticResource;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Mono;

public class NoOpStdKubeApiFacade implements StdKubeApiFacade {

    @Override
    public void deleteNode(String nodeName) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void createNamespacedPod(String namespace, V1Pod pod) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public Mono<V1Pod> createNamespacedPodAsync(String namespace, V1Pod pod) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void deleteNamespacedPod(String namespace, String podName) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void deleteNamespacedPod(String namespace, String podName, int deleteGracePeriod) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void deleteNamespacedPersistentVolumeClaim(String namespace, String volumeClaimName) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void createPersistentVolume(V1PersistentVolume v1PersistentVolume) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void createNamespacedPersistentVolumeClaim(String namespace, V1PersistentVolumeClaim v1PersistentVolumeClaim) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void replacePersistentVolume(V1PersistentVolume persistentVolume) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public void deletePersistentVolume(String volumeName) {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public SharedIndexInformer<V1Node> getNodeInformer() {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public SharedIndexInformer<V1Pod> getPodInformer() {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public SharedIndexInformer<V1PersistentVolume> getPersistentVolumeInformer() {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public SharedIndexInformer<V1PersistentVolumeClaim> getPersistentVolumeClaimInformer() {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public SharedIndexInformer<V1OpportunisticResource> getOpportunisticResourceInformer() {
        throw new IllegalStateException("Kubernetes not supported");
    }

    @Override
    public long getPodInformerStaleness() {
        return -1;
    }

    @Override
    public boolean isReadyForScheduling() {
        return false;
    }
}
