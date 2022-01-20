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
import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.titus.testkit.embedded.kube.event.EmbeddedKubeEvent;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.junit.Test;

import static com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters.RESOURCE_POOL_ELASTIC;
import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedStdKubeApiFacadeTest {

    private static final int DESIRED = 1;

    private final EmbeddedKubeCluster embeddedKubeCluster = EmbeddedKubeClusters.basicCluster(DESIRED);

    private final EmbeddedStdKubeApiFacade integrator = new EmbeddedStdKubeApiFacade(embeddedKubeCluster);

    @Test
    public void testPodInformer() {
        V1Pod pod1 = NodeAndPodCatalog.newPod(RESOURCE_POOL_ELASTIC);
        integrator.createNamespacedPod("default", pod1);

        SharedIndexInformer<V1Pod> podInformer = integrator.getPodInformer();

        // Snapshot
        List<String> podKeys = podInformer.getIndexer().listKeys();
        assertThat(podKeys).hasSize(1);

        LinkedBlockingQueue<EmbeddedKubeEvent<V1Pod>> eventQueue = eventCollector(podInformer);
        List<EmbeddedKubeEvent<V1Pod>> snapshot = drain(eventQueue);
        assertThat(snapshot).hasSize(1);

        // Add
        V1Pod addedPod = NodeAndPodCatalog.newPod(RESOURCE_POOL_ELASTIC);
        integrator.createNamespacedPod("default", addedPod);
        EmbeddedKubeEvent<V1Pod> addedEvent = nextEvent(eventQueue, EmbeddedKubeEvent.Kind.ADDED);
        assertThat(addedEvent.getCurrent()).isEqualTo(addedPod);

        // Update
        V1Pod updatedPod = EmbeddedKubeUtil.copy(addedPod).status(new V1PodStatus().message("updated"));
        embeddedKubeCluster.updatePod(updatedPod);
        EmbeddedKubeEvent<V1Pod> updatedEvent = nextEvent(eventQueue, EmbeddedKubeEvent.Kind.UPDATED);
        assertThat(updatedEvent.getCurrent()).isEqualTo(updatedPod);
        assertThat(updatedEvent.getPrevious()).isEqualTo(addedPod);

        // Delete (do not allow pod termination)
        integrator.deleteNamespacedPod("default", updatedPod.getMetadata().getName());
        nextEvent(eventQueue, EmbeddedKubeEvent.Kind.UPDATED);

        // Delete (allow pod termination)
        embeddedKubeCluster.allowPodTermination(true);
        integrator.deleteNamespacedPod("default", updatedPod.getMetadata().getName());
        nextEvent(eventQueue, EmbeddedKubeEvent.Kind.DELETED);
    }

    @Test
    public void testNodeInformer() {
        SharedIndexInformer<V1Node> nodeInformer = integrator.getNodeInformer();

        // Snapshot
        List<String> nodeKeys = nodeInformer.getIndexer().listKeys();
        assertThat(nodeKeys).hasSize(3 * DESIRED);

        LinkedBlockingQueue<EmbeddedKubeEvent<V1Node>> eventQueue = eventCollector(nodeInformer);
        List<EmbeddedKubeEvent<V1Node>> snapshot = drain(eventQueue);
        assertThat(snapshot).hasSize(3 * DESIRED);

        // Add
        EmbeddedKubeNode addedNode = embeddedKubeCluster.addNodeToServerGroup(EmbeddedKubeClusters.SERVER_GROUP_CRITICAL);
        EmbeddedKubeEvent<V1Node> addedEvent = nextEvent(eventQueue, EmbeddedKubeEvent.Kind.ADDED);
        assertThat(addedEvent.getCurrent()).isEqualTo(addedNode.getV1Node());

        // Update
        EmbeddedKubeNode updatedNode = embeddedKubeCluster.addNode(addedNode.toBuilder().withIpAddress("1.1.1.1").build());
        EmbeddedKubeEvent<V1Node> updatedEvent = nextEvent(eventQueue, EmbeddedKubeEvent.Kind.UPDATED);
        assertThat(updatedEvent.getCurrent()).isEqualTo(updatedNode.getV1Node());
        assertThat(updatedEvent.getPrevious()).isEqualTo(addedNode.getV1Node());

        // Delete
        assertThat(embeddedKubeCluster.deleteNode(updatedNode.getName())).isTrue();
        EmbeddedKubeEvent<V1Node> deletedEvent = nextEvent(eventQueue, EmbeddedKubeEvent.Kind.DELETED);
        assertThat(deletedEvent.getCurrent()).isEqualTo(updatedNode.getV1Node());
    }

    private <T extends KubernetesObject> LinkedBlockingQueue<EmbeddedKubeEvent<T>> eventCollector(SharedIndexInformer<T> nodeInformer) {
        LinkedBlockingQueue<EmbeddedKubeEvent<T>> eventQueue = new LinkedBlockingQueue<>();
        nodeInformer.addEventHandler(new ResourceEventHandler<T>() {
            @Override
            public void onAdd(T node) {
                eventQueue.add(EmbeddedKubeEvent.added(node));
            }

            @Override
            public void onUpdate(T previous, T current) {
                eventQueue.add(EmbeddedKubeEvent.updated(current, previous));
            }

            @Override
            public void onDelete(T node, boolean deletedFinalStateUnknown) {
                eventQueue.add(EmbeddedKubeEvent.deleted(node));
            }
        });
        return eventQueue;
    }

    private <T> EmbeddedKubeEvent<T> nextEvent(LinkedBlockingQueue<EmbeddedKubeEvent<T>> queue, EmbeddedKubeEvent.Kind kind) {
        EmbeddedKubeEvent<T> event = queue.poll();
        assertThat(event).isNotNull();
        assertThat(event.getKind()).isEqualTo(kind);
        return event;
    }

    private <T> List<T> drain(LinkedBlockingQueue<T> queue) {
        List<T> output = new ArrayList<>();
        queue.drainTo(output);
        return output;
    }
}