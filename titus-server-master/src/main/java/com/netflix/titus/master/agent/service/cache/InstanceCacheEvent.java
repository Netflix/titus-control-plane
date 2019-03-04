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

package com.netflix.titus.master.agent.service.cache;

import java.util.Objects;

public class InstanceCacheEvent {

    public enum InstanceCacheEventType {
        // New instance group was detected and added
        InstanceGroupAdded,

        // Existing instance group was removed
        InstanceGroupRemoved,

        // Instance group was updated
        InstanceGroupUpdated
    }

    public static String EMPTY_ID = "empty";

    private final InstanceCacheEventType type;
    private final String resourceId;

    public InstanceCacheEvent(InstanceCacheEventType type, String resourceId) {
        this.type = type;
        this.resourceId = resourceId;
    }

    public InstanceCacheEventType getType() {
        return type;
    }

    public String getResourceId() {
        return resourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InstanceCacheEvent that = (InstanceCacheEvent) o;
        return type == that.type &&
                Objects.equals(resourceId, that.resourceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, resourceId);
    }

    @Override
    public String toString() {
        return "InstanceCacheEvent{" +
                "type=" + type +
                ", resourceId='" + resourceId + '\'' +
                '}';
    }
}
