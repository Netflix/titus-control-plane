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

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.testkit.embedded.kube.event.EmbeddedKubeEvent;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStateWaiting;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodIP;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class EmbeddedKubeCluster {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKubeCluster.class);

    private final EmbeddedKubeFleet embeddedKubeFleet = new EmbeddedKubeFleet();
    private final EmbeddedKubeApiFacade kubeIntegrator;
    private final EmbeddedKubeScheduler kubeScheduler;

    private final ConcurrentMap<String, EmbeddedKubePod> pods = new ConcurrentHashMap<>();
    private final Sinks.Many<EmbeddedKubeEvent<V1Pod>> podPublisher = Sinks.many().multicast().directAllOrNothing();

    private volatile boolean allowPodTermination;

    public EmbeddedKubeCluster() {
        this.kubeIntegrator = new EmbeddedKubeApiFacade(this);
        this.kubeScheduler = new EmbeddedKubeScheduler(this);
    }

    public EmbeddedKubeApiFacade getKubeApiFacade() {
        return kubeIntegrator;
    }

    public EmbeddedKubeFleet getFleet() {
        return embeddedKubeFleet;
    }

    public void addServerGroup(EmbeddedServerGroup serverGroup) {
        embeddedKubeFleet.addServerGroup(serverGroup);
    }

    public EmbeddedKubeNode addNodeToServerGroup(String serverGroupName) {
        return embeddedKubeFleet.addNodeToServerGroup(serverGroupName);
    }

    public EmbeddedKubeNode addNode(EmbeddedKubeNode node) {
        return embeddedKubeFleet.addNode(node);
    }

    public boolean deleteNode(String name) {
        return embeddedKubeFleet.deleteNode(name);
    }

    public void addPod(V1Pod pod) {
        String podName = pod.getMetadata().getName();
        pod.status(new V1PodStatus().phase("PENDING").containerStatuses(new ArrayList<>()));
        pod.getMetadata().creationTimestamp(OffsetDateTime.now());

        EmbeddedKubePod embeddedPod = new EmbeddedKubePod(pod);

        synchronized (pods) {
            EmbeddedKubePod previous = pods.put(podName, embeddedPod);
            if (previous == null) {
                logger.info("Adding pod: {}", podName);
                podPublisher.tryEmitNext(EmbeddedKubeEvent.added(pod));
            } else {
                logger.info("Updating pod: {}", podName);
                podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(pod, previous.getV1Pod()));
            }
        }
    }

    public void updatePod(V1Pod pod) {
        String podName = pod.getMetadata().getName();
        synchronized (pods) {
            logger.info("Updating pod: {}", podName);
            EmbeddedKubePod previous = Preconditions.checkNotNull(pods.get(podName));
            pods.put(podName, new EmbeddedKubePod(pod));
            podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(pod, previous.getV1Pod()));
        }
    }

    public boolean deletePod(String podName, long deleteGracePeriod) {
        synchronized (pods) {
            if (allowPodTermination && deleteGracePeriod <= 0) {
                EmbeddedKubePod removed = pods.remove(podName);
                if (removed == null) {
                    return false;
                }
                logger.info("Deleted pod: {}", podName);
                podPublisher.tryEmitNext(EmbeddedKubeEvent.deleted(removed.getV1Pod()));
            } else {
                moveToKillInitiatedState(podName, deleteGracePeriod);
            }
        }
        return true;
    }

    public Map<String, EmbeddedKubePod> getEmbeddedPods() {
        return new HashMap<>(pods);
    }

    public Map<String, V1Pod> getPods() {
        return pods.values().stream()
                .map(EmbeddedKubePod::getV1Pod)
                .collect(Collectors.toMap(p -> p.getMetadata().getName(), p -> p));
    }

    public String getPlacementZone(String podName) {
        EmbeddedKubePod embeddedKubePod = pods.get(podName);
        if (embeddedKubePod == null || embeddedKubePod.getNodeName() == null) {
            return null;
        }
        return getFleet().getNodes().get(embeddedKubePod.getNodeName()).getZone();
    }

    public void schedule() {
        kubeScheduler.doSchedule();
    }

    public void assignPodToNode(String podName, EmbeddedKubeNode node) {
        synchronized (pods) {
            EmbeddedKubePod embeddedKubePod = Preconditions.checkNotNull(pods.get(podName), "Pod not found: podName=%s", podName);
            V1Pod pod = embeddedKubePod.getV1Pod();
            embeddedKubeFleet.assignPodToNode(pod, node);
            V1Pod updatedPod = EmbeddedKubeUtil.copy(pod);
            updatedPod.getSpec()
                    .nodeName(node.getName());
            updatedPod.getStatus()
                    .nominatedNodeName(node.getName())
                    .containerStatuses(Collections.singletonList(
                            new V1ContainerStatus().ready(true)
                    ))
                    .phase("Pending")
                    .reason("SCHEDULED");
            pods.put(podName, new EmbeddedKubePod(updatedPod));
            logger.info("Pod assigned to a node: podName={}, nodeName={}, nodeZone={}", podName, node.getName(), node.getZone());
            podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(updatedPod, pod));
        }
    }

    public void moveToStartInitiatedState(String podName) {
        synchronized (pods) {
            EmbeddedKubePod embeddedKubePod = Preconditions.checkNotNull(pods.get(podName), "Pod not found: podName=%s", podName);
            Preconditions.checkNotNull(embeddedKubePod.getNodeName(), "Pod not assigned to any node: podName=%s", podName);
            V1Pod pod = embeddedKubePod.getV1Pod();

            if ("TASK_STARTING".equals(pod.getStatus().getReason())) {
                return;
            }

            V1Pod updatedPod = EmbeddedKubeUtil.copy(pod);
            String podIpAddress = EmbeddedKubeUtil.nextIpAddress();
            updatedPod.getStatus()
                    .podIP(podIpAddress)
                    .podIPs(Collections.singletonList(new V1PodIP().ip(podIpAddress)))
                    .containerStatuses(Collections.singletonList(
                            new V1ContainerStatus()
                                    .ready(true)
                                    .name("test task")
                                    .state(new V1ContainerState().waiting(
                                            new V1ContainerStateWaiting()
                                    ))
                    ))
                    .phase("Pending")
                    .reason("TASK_STARTING");
            pods.put(podName, new EmbeddedKubePod(updatedPod));
            logger.info("Moved pod starting state: {}", podName);
            podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(updatedPod, pod));
        }
    }

    public void moveToStartedState(String podName) {
        synchronized (pods) {
            EmbeddedKubePod embeddedKubePod = Preconditions.checkNotNull(pods.get(podName), "Pod not found: podName=%s", podName);
            Preconditions.checkNotNull(embeddedKubePod.getNodeName(), "Pod not assigned to any node: podName=%s", podName);
            V1Pod pod = embeddedKubePod.getV1Pod();

            if ("TASK_RUNNING".equals(pod.getStatus().getReason())) {
                return;
            }

            V1Pod updatedPod = EmbeddedKubeUtil.copy(pod);
            updatedPod.getStatus()
                    .containerStatuses(Collections.singletonList(
                            new V1ContainerStatus()
                                    .started(true)
                                    .state(new V1ContainerState().running(
                                            new V1ContainerStateRunning().startedAt(OffsetDateTime.now())
                                    ))
                    ))
                    .phase("Running")
                    .reason("TASK_RUNNING");
            pods.put(podName, new EmbeddedKubePod(updatedPod));
            logger.info("Moved pod running state: {}", podName);
            podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(updatedPod, pod));
        }
    }

    public void moveToKillInitiatedState(String podName, long deleteGracePeriodMs) {
        synchronized (pods) {
            EmbeddedKubePod embeddedKubePod = Preconditions.checkNotNull(pods.get(podName), "Pod not found: podName=%s", podName);
            V1Pod pod = embeddedKubePod.getV1Pod();

            V1Pod updatedPod = EmbeddedKubeUtil.copy(pod);
            updatedPod.getMetadata()
                    .deletionGracePeriodSeconds(deleteGracePeriodMs / 1_000)
                    .deletionTimestamp(OffsetDateTime.now());
            pods.put(podName, new EmbeddedKubePod(updatedPod));
            logger.info("Moved pod killInitiated state: {}", podName);
            podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(updatedPod, pod));
        }
    }

    public void moveToFinishedSuccess(String podName) {
        moveToFinishedState(podName, true, "OK");
    }

    public void moveToFinishedFailed(String podName, String message) {
        moveToFinishedState(podName, false, message);
    }

    private void moveToFinishedState(String podName, boolean succeeded, String message) {
        synchronized (pods) {
            EmbeddedKubePod embeddedKubePod = Preconditions.checkNotNull(pods.get(podName), "Pod not found: podName=%s", podName);
            V1Pod pod = embeddedKubePod.getV1Pod();

            V1Pod updatedPod = EmbeddedKubeUtil.copy(pod);
            updatedPod.getStatus()
                    .containerStatuses(Collections.singletonList(
                            new V1ContainerStatus()
                                    .started(true)
                                    .state(new V1ContainerState().terminated(
                                            new V1ContainerStateTerminated().finishedAt(OffsetDateTime.now())
                                    ))
                    ));

            if (!pod.getStatus().getPhase().equals("Running")) {
                updatedPod.getStatus()
                        .phase("Failed")
                        .reason(message);
            } else if (succeeded) {
                updatedPod.getStatus()
                        .phase("Succeeded")
                        .reason(message);
            } else {
                updatedPod.getStatus()
                        .phase("Failed")
                        .reason(message);
            }
            pods.put(podName, new EmbeddedKubePod(updatedPod));

            if (pod.getSpec().getNodeName() != null) {
                embeddedKubeFleet.removePodFromNode(pod);
            }

            logger.info("Moved pod finished state: {}", podName);
            podPublisher.tryEmitNext(EmbeddedKubeEvent.updated(updatedPod, pod));
        }
    }

    public void removePod(String podName) {
        pods.remove(podName);
    }

    public void allowPodTermination(boolean allow) {
        this.allowPodTermination = allow;
    }

    public Flux<EmbeddedKubeEvent<V1Pod>> observePods() {
        return podPublisher.asFlux();
    }
}
