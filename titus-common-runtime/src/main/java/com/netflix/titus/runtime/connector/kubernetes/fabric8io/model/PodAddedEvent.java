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

import io.fabric8.kubernetes.api.model.Pod;

public class PodAddedEvent extends PodEvent {
    PodAddedEvent(Pod pod) {
        super(pod);
    }

    @Override
    public String toString() {
        return "PodAddedEvent{" +
                "taskId=" + taskId +
                ", sequenceNumber=" + sequenceNumber +
                ", pod=" + F8KubeObjectFormatter.formatPodEssentials(pod) +
                '}';
    }
}
