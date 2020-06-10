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

package com.netflix.titus.master.mesos.kubeapiserver.direct.model;

import java.util.Optional;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1Pod;

/**
 * A helper object for processing pod state. This implementation assumes that it is always at most one container
 * in a pod.
 */
public class PodWrapper {

    private final V1Pod v1Pod;

    private volatile PodPhase podPhase;

    public PodWrapper(V1Pod v1Pod) {
        this.v1Pod = v1Pod;
    }

    public V1Pod getV1Pod() {
        return v1Pod;
    }

    public PodPhase getPodPhase() {
        if (podPhase == null) {
            podPhase = v1Pod.getStatus() != null && v1Pod.getStatus().getPhase() != null
                    ? PodPhase.parse(v1Pod.getStatus().getPhase())
                    : PodPhase.UNKNOWN;
        }
        return podPhase;
    }

    public String getMessage() {
        return v1Pod.getStatus() != null && v1Pod.getStatus().getMessage() != null
                ? v1Pod.getStatus().getMessage()
                : "<no message>";
    }

    public Optional<V1ContainerState> findContainerState() {
        return KubeUtil.findContainerState(v1Pod);
    }

    public Optional<Long> findFinishedAt() {
        V1ContainerState containerState = findContainerState().orElse(null);
        if (containerState != null && containerState.getTerminated() != null && containerState.getTerminated().getFinishedAt() != null) {
            return Optional.of(containerState.getTerminated().getFinishedAt().getMillis());
        }
        return Optional.empty();
    }

    public Optional<String> findPodAnnotation(String key) {
        if (v1Pod.getMetadata() == null || v1Pod.getMetadata().getAnnotations() == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(v1Pod.getMetadata().getAnnotations().get(key));
    }

    public boolean hasContainers() {
        return v1Pod.getStatus() != null && !CollectionsExt.isNullOrEmpty(v1Pod.getStatus().getContainerStatuses());
    }

    public boolean hasDeletionTimestamp() {
        return v1Pod.getMetadata() != null && v1Pod.getMetadata().getDeletionTimestamp() != null;

    }

    public boolean isTerminated() {
        PodPhase podPhase = getPodPhase();
        return podPhase == PodPhase.FAILED || podPhase == PodPhase.SUCCEEDED;
    }
}
