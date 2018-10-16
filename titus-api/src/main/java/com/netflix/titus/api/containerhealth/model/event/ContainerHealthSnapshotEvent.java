/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.api.containerhealth.model.event;

import java.util.List;
import java.util.Objects;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;

public class ContainerHealthSnapshotEvent extends ContainerHealthEvent {

    private final List<ContainerHealthStatus> snapshot;

    public ContainerHealthSnapshotEvent(List<ContainerHealthStatus> snapshot) {
        this.snapshot = snapshot;
    }

    public List<ContainerHealthStatus> getSnapshot() {
        return snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerHealthSnapshotEvent that = (ContainerHealthSnapshotEvent) o;
        return Objects.equals(snapshot, that.snapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot);
    }

    @Override
    public String toString() {
        return "ContainerHealthSnapshotEvent{" +
                "snapshot=" + snapshot +
                '}';
    }
}
