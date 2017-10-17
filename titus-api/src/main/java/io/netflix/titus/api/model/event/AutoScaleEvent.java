/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.model.event;

/**
 * Autoscaler base event type.
 */
public abstract class AutoScaleEvent {

    private final String instanceGroupId;

    protected AutoScaleEvent(String instanceGroupId) {
        this.instanceGroupId = instanceGroupId;
    }

    public String getInstanceGroupId() {
        return instanceGroupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AutoScaleEvent that = (AutoScaleEvent) o;

        return instanceGroupId != null ? instanceGroupId.equals(that.instanceGroupId) : that.instanceGroupId == null;
    }

    @Override
    public int hashCode() {
        return instanceGroupId != null ? instanceGroupId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "AutoScaleEvent{" +
                "instanceGroupId='" + instanceGroupId + '\'' +
                '}';
    }
}
