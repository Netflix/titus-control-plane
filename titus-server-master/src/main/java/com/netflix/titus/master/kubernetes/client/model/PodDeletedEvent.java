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

package com.netflix.titus.master.kubernetes.client.model;

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.master.kubernetes.KubeObjectFormatter;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;

public class PodDeletedEvent extends PodEvent {

    private final boolean deletedFinalStateUnknown;
    private final Optional<V1Node> node;

    PodDeletedEvent(V1Pod pod, boolean deletedFinalStateUnknown, Optional<V1Node> node) {
        super(pod);
        this.deletedFinalStateUnknown = deletedFinalStateUnknown;
        this.node = node;
    }

    public boolean isDeletedFinalStateUnknown() {
        return deletedFinalStateUnknown;
    }

    public Optional<V1Node> getNode() {
        return node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PodDeletedEvent that = (PodDeletedEvent) o;
        return deletedFinalStateUnknown == that.deletedFinalStateUnknown &&
                Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), deletedFinalStateUnknown, node);
    }

    @Override
    public String toString() {
        return "PodDeletedEvent{" +
                "taskId='" + taskId + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + KubeObjectFormatter.formatPodEssentials(pod) +
                ", deletedFinalStateUnknown=" + deletedFinalStateUnknown +
                ", node=" + node.map(n -> n.getMetadata().getName()).orElse("<not_assigned>") +
                '}';
    }
}
