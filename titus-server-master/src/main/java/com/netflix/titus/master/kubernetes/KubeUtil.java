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

package com.netflix.titus.master.kubernetes;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.NetworkExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStateWaiting;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Toleration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubeUtil {

    private static final Logger logger = LoggerFactory.getLogger(KubeUtil.class);

    private static final String SUCCEEDED = "Succeeded";

    private static final String FAILED = "Failed";

    private static final String BOUND = "Bound";

    public static final String TYPE_INTERNAL_IP = "InternalIP";

    private static final JsonFormat.Printer grpcJsonPrinter = JsonFormat.printer().includingDefaultValueFields();

    private static final Gson GSON = new Gson();

    /**
     * As it is not possible to capture pod size at the transport level, we try to estimate it directly using the same
     * JSON serializer as the Kube client (gson).
     */
    public static int estimatePodSize(V1Pod v1Pod) {
        try {
            String json = GSON.toJson(v1Pod);
            return json == null ? 0 : json.length();
        } catch (Exception e) {
            return 0;
        }
    }

    public static boolean isPodPhaseTerminal(String phase) {
        return SUCCEEDED.equals(phase) || FAILED.equals(phase);
    }

    public static boolean isPersistentVolumeBound(String phase) {
        return BOUND.equals(phase);
    }

    public static Optional<Long> findFinishedTimestamp(V1Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return Optional.empty();
        }
        return pod.getStatus().getContainerStatuses().stream()
                .filter(status -> status.getState() != null && status.getState().getTerminated() != null && status.getState().getTerminated().getFinishedAt() != null)
                .findFirst()
                .map(terminatedState -> terminatedState.getState().getTerminated().getFinishedAt().toInstant().toEpochMilli());
    }

    public static Optional<TitusExecutorDetails> getTitusExecutorDetails(V1Pod pod) {
        if (pod.getMetadata() == null || pod.getMetadata().getAnnotations() == null) {
            return Optional.empty();
        }
        Map<String, String> annotations = pod.getMetadata().getAnnotations();
        if (!Strings.isNullOrEmpty(annotations.get("IpAddress"))) {
            TitusExecutorDetails titusExecutorDetails = new TitusExecutorDetails(
                    Collections.emptyMap(),
                    new TitusExecutorDetails.NetworkConfiguration(
                            Boolean.parseBoolean(annotations.getOrDefault("IsRoutableIp", "true")),
                            annotations.getOrDefault("IpAddress", "UnknownIpAddress"),
                            annotations.get("EniIPv6Address"),
                            annotations.getOrDefault("EniIpAddress", "UnknownEniIpAddress"),
                            annotations.getOrDefault("NetworkMode", "UnknownNetworkMode"),
                            annotations.getOrDefault("EniId", "UnknownEniId"),
                            annotations.getOrDefault("ResourceId", "UnknownResourceId")
                    )
            );
            return Optional.of(titusExecutorDetails);
        }
        return Optional.empty();
    }

    public static Optional<V1ContainerState> findContainerState(V1Pod pod) {
        if (pod.getStatus() == null) {
            return Optional.empty();
        }
        List<V1ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
        if (containerStatuses != null) {
            for (V1ContainerStatus status : containerStatuses) {
                V1ContainerState state = status.getState();
                if (state != null) {
                    return Optional.of(state);
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<V1ContainerStateTerminated> findTerminatedContainerStatus(V1Pod pod) {
        return findContainerState(pod).flatMap(state -> Optional.ofNullable(state.getTerminated()));
    }

    public static String formatV1ContainerState(V1ContainerState containerState) {
        if (containerState.getWaiting() != null) {
            V1ContainerStateWaiting waiting = containerState.getWaiting();
            return String.format("{state=waiting, reason=%s, message=%s}", waiting.getReason(), waiting.getMessage());
        }

        if (containerState.getRunning() != null) {
            V1ContainerStateRunning running = containerState.getRunning();
            return String.format("{state=running, startedAt=%s}", running.getStartedAt());
        }

        if (containerState.getTerminated() != null) {
            V1ContainerStateTerminated terminated = containerState.getTerminated();
            return String.format("{state=terminated, startedAt=%s, finishedAt=%s, reason=%s, message=%s}",
                    terminated.getStartedAt(), terminated.getFinishedAt(),
                    terminated.getReason(), terminated.getMessage());
        }

        return "{state=<not set>}";
    }

    /**
     * If a job has an availability zone hard constraint with a farzone id, return this farzone id.
     */
    public static Optional<String> findFarzoneId(KubePodConfiguration configuration, Job<?> job) {
        List<String> farzones = configuration.getFarzones();
        if (CollectionsExt.isNullOrEmpty(farzones)) {
            return Optional.empty();
        }

        String zone = JobFunctions.findHardConstraint(job, JobConstraints.AVAILABILITY_ZONE).orElse("");
        if (StringExt.isEmpty(zone)) {
            return Optional.empty();
        }

        for (String farzone : farzones) {
            if (zone.equalsIgnoreCase(farzone)) {
                return Optional.of(farzone);
            }
        }
        return Optional.empty();
    }

    public static boolean isOwnedByKubeScheduler(V1Pod v1Pod) {
        List<V1Toleration> tolerations = v1Pod.getSpec().getTolerations();
        if (CollectionsExt.isNullOrEmpty(tolerations)) {
            return false;
        }
        for (V1Toleration toleration : tolerations) {
            if (KubeConstants.TAINT_SCHEDULER.equals(toleration.getKey()) && KubeConstants.TAINT_SCHEDULER_VALUE_KUBE.equals(toleration.getValue())) {
                return true;
            }
        }
        return false;
    }

    public static Optional<String> getNodeIpV4Address(V1Node node) {
        return Optional.ofNullable(node.getStatus().getAddresses())
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .filter(a -> a.getType().equalsIgnoreCase(TYPE_INTERNAL_IP) && NetworkExt.isIpV4(a.getAddress()))
                .findFirst()
                .map(V1NodeAddress::getAddress);
    }

    public static boolean isFarzoneNode(List<String> farzones, V1Node node) {
        Map<String, String> labels = node.getMetadata().getLabels();
        if (CollectionsExt.isNullOrEmpty(labels)) {
            return false;
        }
        String nodeZone = labels.get(KubeConstants.NODE_LABEL_ZONE);
        if (StringExt.isEmpty(nodeZone)) {
            logger.debug("Node without zone label: {}", node.getMetadata().getName());
            return false;
        }
        for (String farzone : farzones) {
            if (farzone.equalsIgnoreCase(nodeZone)) {
                logger.debug("Farzone node: nodeId={}, zoneId={}", node.getMetadata().getName(), nodeZone);
                return true;
            }
        }
        logger.debug("Non-farzone node: nodeId={}, zoneId={}", node.getMetadata().getName(), nodeZone);
        return false;
    }

    public static String toErrorDetails(Throwable e) {
        if (!(e instanceof ApiException)) {
            return ExceptionExt.toMessageChain(e);
        }

        ApiException apiException = (ApiException) e;
        return String.format("{message=%s, httpCode=%d, responseBody=%s",
                Evaluators.getOrDefault(apiException.getMessage(), "<not set>"),
                apiException.getCode(),
                Evaluators.getOrDefault(apiException.getResponseBody(), "<not set>")
        );
    }

    /**
     * Get Kube object name
     */
    @Deprecated
    public static String getMetadataName(V1ObjectMeta metadata) {
        return com.netflix.titus.runtime.connector.kubernetes.KubeUtil.getMetadataName(metadata);
    }

    public static Optional<V1NodeCondition> findNodeCondition(V1Node node, String type) {
        V1NodeStatus status = node.getStatus();
        if (status == null) {
            return Optional.empty();
        }
        List<V1NodeCondition> conditions = status.getConditions();
        if (conditions != null) {
            for (V1NodeCondition condition : conditions) {
                if (condition.getType().equals(type)) {
                    return Optional.of(condition);
                }
            }
        }
        return Optional.empty();
    }

    public static boolean hasUninitializedTaint(V1Node node) {
        if (node.getSpec() != null && node.getSpec().getTaints() != null) {
            return node.getSpec().getTaints().stream()
                    .anyMatch(t -> KubeConstants.TAINT_NODE_UNINITIALIZED.equals(t.getKey()));
        }
        return false;
    }
}
