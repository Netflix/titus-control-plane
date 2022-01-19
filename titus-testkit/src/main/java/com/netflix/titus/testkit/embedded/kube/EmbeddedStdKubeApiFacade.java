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

package com.netflix.titus.testkit.embedded.kube;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.runtime.connector.kubernetes.KubeApiException;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import com.netflix.titus.testkit.embedded.kube.event.SharedInformerStub;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;
import reactor.core.publisher.Mono;

public class EmbeddedStdKubeApiFacade implements StdKubeApiFacade {

    private final EmbeddedKubeCluster embeddedKubeCluster;
    private final EmbeddedKubeFleet fleet;

    public EmbeddedStdKubeApiFacade(EmbeddedKubeCluster embeddedKubeCluster) {
        fleet = embeddedKubeCluster.getFleet();
        this.embeddedKubeCluster = embeddedKubeCluster;
    }

    @Override
    public void deleteNode(String nodeName) throws KubeApiException {
    }

    @Override
    public SharedIndexInformer<V1Node> getNodeInformer() {
        List<V1Node> snapshot = fleet.getNodes().values().stream().map(EmbeddedKubeNode::getV1Node).collect(Collectors.toList());
        return new SharedInformerStub<>(snapshot, fleet.observeNodes().map(event -> event.map(EmbeddedKubeNode::getV1Node)));
    }

    @Override
    public void createNamespacedPod(String namespace, V1Pod pod) throws KubeApiException {
        embeddedKubeCluster.addPod(pod);
    }

    @Override
    public Mono<V1Pod> createNamespacedPodAsync(String namespace, V1Pod pod) {
        return Mono.fromRunnable(() -> {
            embeddedKubeCluster.addPod(pod);
        });
    }

    @Override
    public void deleteNamespacedPod(String namespace, String podName) throws KubeApiException {
        embeddedKubeCluster.deletePod(podName, 0);
    }

    @Override
    public void deleteNamespacedPod(String namespace, String podName, int deleteGracePeriod) throws KubeApiException {
        embeddedKubeCluster.deletePod(podName, deleteGracePeriod);
    }

    @Override
    public SharedIndexInformer<V1Pod> getPodInformer() {
        List<V1Pod> snapshot = new ArrayList<>(embeddedKubeCluster.getPods().values());
        return new SharedInformerStub<>(snapshot, embeddedKubeCluster.observePods());
    }

    @Override
    public void createPersistentVolume(V1PersistentVolume v1PersistentVolume) throws KubeApiException {

    }

    @Override
    public void createNamespacedPersistentVolumeClaim(String namespace, V1PersistentVolumeClaim v1PersistentVolumeClaim) throws KubeApiException {

    }

    @Override
    public void deleteNamespacedPersistentVolumeClaim(String namespace, String volumeClaimName) throws KubeApiException {

    }

    @Override
    public void replacePersistentVolume(V1PersistentVolume persistentVolume) throws KubeApiException {

    }

    @Override
    public void deletePersistentVolume(String volumeName) throws KubeApiException {

    }

    @Override
    public SharedIndexInformer<V1PersistentVolume> getPersistentVolumeInformer() {
        return null;
    }

    @Override
    public SharedIndexInformer<V1PersistentVolumeClaim> getPersistentVolumeClaimInformer() {
        return null;
    }
}
