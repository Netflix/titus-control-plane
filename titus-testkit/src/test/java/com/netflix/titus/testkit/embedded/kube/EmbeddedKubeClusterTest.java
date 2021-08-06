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

import java.util.Iterator;

import com.netflix.titus.testkit.embedded.kube.event.EmbeddedKubeEvent;
import io.kubernetes.client.openapi.models.V1Pod;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedKubeClusterTest {

    private static final int DESIRED = 1;

    private final EmbeddedKubeCluster embeddedKubeCluster = EmbeddedKubeClusters.basicCluster(DESIRED);

    @Test
    public void testRunPod() {
        V1Pod pod1 = NodeAndPodCatalog.newPod();
        String pod1Name = pod1.getMetadata().getName();

        Iterator<EmbeddedKubeEvent<V1Pod>> podEventIt = embeddedKubeCluster.observePods().toIterable().iterator();
        embeddedKubeCluster.addPod(pod1);
        expectPodEvent(podEventIt, EmbeddedKubeEvent.Kind.ADDED, pod1);

        // Scheduled
        embeddedKubeCluster.schedule();
        V1Pod pod1Scheduled = embeddedKubeCluster.getPods().get(pod1Name);
        assertThat(pod1Scheduled.getStatus().getReason()).isEqualTo("SCHEDULED");
        expectPodEvent(podEventIt, EmbeddedKubeEvent.Kind.UPDATED, pod1Scheduled);

        String assignedNode = pod1Scheduled.getSpec().getNodeName();
        assertThat(embeddedKubeCluster.getFleet().getNodes()).containsKey(assignedNode);

        // StartInitiated
        embeddedKubeCluster.moveToStartInitiatedState(pod1Name);
        V1Pod pod1StartInitiated = embeddedKubeCluster.getPods().get(pod1Name);
        assertThat(pod1StartInitiated.getStatus().getPodIP()).isNotNull();
        expectPodEvent(podEventIt, EmbeddedKubeEvent.Kind.UPDATED, pod1StartInitiated);

        // Started
        embeddedKubeCluster.moveToStartedState(pod1Name);
        V1Pod pod1Started = embeddedKubeCluster.getPods().get(pod1Name);
        assertThat(pod1Started.getStatus().getPhase()).isEqualTo("Running");
        expectPodEvent(podEventIt, EmbeddedKubeEvent.Kind.UPDATED, pod1Started);

        EmbeddedKubeNode nodeAfterStarted = embeddedKubeCluster.getFleet().getNodes().get(assignedNode);
        assertThat(nodeAfterStarted.getAssignedPods()).hasSize(1);

        // Finished
        embeddedKubeCluster.moveToFinishedSuccess(pod1Name);
        V1Pod pod1Finished = embeddedKubeCluster.getPods().get(pod1Name);
        assertThat(pod1Finished.getStatus().getPhase()).isEqualTo("Succeeded");
        expectPodEvent(podEventIt, EmbeddedKubeEvent.Kind.UPDATED, pod1Finished);

        EmbeddedKubeNode nodeAfterFinished = embeddedKubeCluster.getFleet().getNodes().get(assignedNode);
        assertThat(nodeAfterFinished.getAssignedPods()).isEmpty();

        // Remove pod
        embeddedKubeCluster.removePod(pod1Name);
        assertThat(embeddedKubeCluster.getPods()).doesNotContainKey(pod1Name);
    }

    @Test
    public void testRunAndTerminatePod() {
        V1Pod pod1 = NodeAndPodCatalog.newPod();
        String pod1Name = pod1.getMetadata().getName();

        embeddedKubeCluster.addPod(pod1);
        embeddedKubeCluster.schedule();
        embeddedKubeCluster.moveToStartInitiatedState(pod1Name);
        embeddedKubeCluster.moveToStartedState(pod1Name);
        embeddedKubeCluster.moveToKillInitiatedState(pod1Name, 60_000);

        // Kill initiated
        V1Pod podKillInitiated = embeddedKubeCluster.getPods().get(pod1Name);
        assertThat(podKillInitiated.getMetadata().getDeletionGracePeriodSeconds()).isEqualTo(60L);
        assertThat(podKillInitiated.getMetadata().getDeletionTimestamp()).isNotNull();

        // Finished
        embeddedKubeCluster.moveToFinishedFailed(pod1Name, "failed");
        V1Pod pod1Finished = embeddedKubeCluster.getPods().get(pod1Name);
        assertThat(pod1Finished.getStatus().getPhase()).isEqualTo("Failed");
    }

    private void expectPodEvent(Iterator<EmbeddedKubeEvent<V1Pod>> eventIt, EmbeddedKubeEvent.Kind expectedKind, V1Pod expected) {
        assertThat(eventIt.hasNext()).isTrue();
        EmbeddedKubeEvent<V1Pod> event = eventIt.next();
        assertThat(event.getKind()).isEqualTo(expectedKind);
        assertThat(event.getCurrent()).isEqualTo(expected);
    }
}