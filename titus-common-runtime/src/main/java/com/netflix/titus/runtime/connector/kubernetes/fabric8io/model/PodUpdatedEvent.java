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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io.model;

import java.util.Objects;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;

public class PodUpdatedEvent extends PodEvent {

    private final Pod oldPod;
    private final Optional<Node> node;

    PodUpdatedEvent(Pod oldPod, Pod newPod, Optional<Node> node) {
        super(newPod);
        this.oldPod = oldPod;
        this.node = node;
    }

    public Pod getOldPod() {
        return oldPod;
    }

    public Optional<Node> getNode() {
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
        PodUpdatedEvent that = (PodUpdatedEvent) o;
        return Objects.equals(oldPod, that.oldPod) &&
                Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oldPod, node);
    }

    @Override
    public String toString() {
        return "PodUpdatedEvent{" +
                "taskId='" + taskId + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + F8KubeObjectFormatter.formatPodEssentials(pod) +
                ", oldPod=" + F8KubeObjectFormatter.formatPodEssentials(oldPod) +
                ", node=" + node.map(n -> n.getMetadata().getName()).orElse("<not_assigned>") +
                '}';
    }
}
