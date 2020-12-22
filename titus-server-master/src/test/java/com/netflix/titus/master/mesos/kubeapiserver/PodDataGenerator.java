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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import com.netflix.titus.common.util.CollectionsExt;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStateWaiting;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.joda.time.DateTime;

public class PodDataGenerator {

    @SafeVarargs
    public static V1Pod newPod(String taskId, Function<V1Pod, V1Pod>... transformers) {
        V1Pod pod = new V1Pod()
                .metadata(new V1ObjectMeta()
                        .name(taskId)
                )
                .spec(new V1PodSpec())
                .status(new V1PodStatus()
                        .addContainerStatusesItem(new V1ContainerStatus())
                );
        return transform(pod, transformers);
    }

    @SafeVarargs
    public static V1Pod newPod(Function<V1Pod, V1Pod>... transformers) {
        return newPod("task1", transformers);
    }

    @SafeVarargs
    public static V1Pod transform(V1Pod pod, Function<V1Pod, V1Pod>... transformers) {
        for (Function<V1Pod, V1Pod> transformer : transformers) {
            transformer.apply(pod);
        }
        return pod;
    }

    public static V1Pod andPodAnnotations(V1Pod pod, String... keyValuePairs) {
        return andPodAnnotations(keyValuePairs).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andPodAnnotations(String... keyValuePairs) {
        return pod -> {
            Map<String, String> annotations = CollectionsExt.copyAndAdd(
                    CollectionsExt.nonNull(pod.getMetadata().getAnnotations()),
                    CollectionsExt.asMap(keyValuePairs)
            );
            pod.getMetadata().annotations(annotations);
            return pod;
        };
    }

    public static V1Pod andPhase(String phase, V1Pod pod) {
        return andPhase(phase).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andPhase(String phase) {
        return pod -> {
            pod.getStatus().phase(phase);
            return pod;
        };
    }

    public static V1Pod andNodeName(String nodeName, V1Pod pod) {
        return andNodeName(nodeName).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andNodeName(String nodeName) {
        return pod -> {
            pod.getSpec().setNodeName(nodeName);
            return pod;
        };
    }

    public static V1Pod andPodIp(String podIp, V1Pod pod) {
        return andPodIp(podIp).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andPodIp(String podIp) {
        return pod -> {
            pod.getStatus().setPodIP(podIp);
            return pod;
        };
    }

    public static V1Pod andScheduled(V1Pod pod) {
        return andScheduled().apply(pod);
    }

    public static Function<V1Pod, V1Pod> andScheduled() {
        return pod -> {
            pod.getSpec().nodeName(NodeDataGenerator.NODE_NAME);
            return pod;
        };
    }

    public static V1Pod andWaiting(V1Pod pod) {
        return andWaiting().apply(pod);
    }

    public static Function<V1Pod, V1Pod> andWaiting() {
        return pod -> {
            pod.getStatus().containerStatuses(Collections.singletonList(
                    new V1ContainerStatus().state(new V1ContainerState().waiting(new V1ContainerStateWaiting()))
            ));
            return pod;
        };
    }

    public static V1Pod andStartedAt(long timestamp, V1Pod pod) {
        return andStartedAt(timestamp).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andStartedAt(long timestamp) {
        return pod -> {
            pod.getStatus().containerStatuses(Collections.singletonList(
                    new V1ContainerStatus().state(
                            new V1ContainerState().running(
                                    new V1ContainerStateRunning().startedAt(new DateTime(timestamp))
                            )
                    )
            ));
            return pod;
        };
    }

    public static V1Pod andRunning(V1Pod pod) {
        return andRunning().apply(pod);
    }

    public static Function<V1Pod, V1Pod> andRunning() {
        return pod -> andStartedAt(System.currentTimeMillis(), pod);
    }

    public static V1Pod andDeletionTimestamp(V1Pod pod) {
        return andDeletionTimestamp().apply(pod);
    }

    public static Function<V1Pod, V1Pod> andDeletionTimestamp() {
        return pod -> {
            if (pod.getMetadata() == null) {
                pod.metadata(new V1ObjectMeta());
            }
            pod.getMetadata().deletionTimestamp(DateTime.now());
            return pod;
        };
    }

    public static V1Pod andTerminated(V1Pod pod) {
        return andTerminated().apply(pod);
    }

    public static Function<V1Pod, V1Pod> andTerminated() {
        return pod -> {
            pod.getStatus().containerStatuses(Collections.singletonList(
                    new V1ContainerStatus().state(new V1ContainerState().terminated(new V1ContainerStateTerminated()))
            ));
            return pod;
        };
    }

    public static V1Pod andReason(String reason, V1Pod pod) {
        return andReason(reason).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andReason(String reason) {
        return pod -> {
            pod.getStatus().reason(reason);
            return pod;
        };
    }

    public static V1Pod andMessage(String message, V1Pod pod) {
        return andMessage(message).apply(pod);
    }

    public static Function<V1Pod, V1Pod> andMessage(String message) {
        return pod -> {
            pod.getStatus().message(message);
            return pod;
        };
    }
}
