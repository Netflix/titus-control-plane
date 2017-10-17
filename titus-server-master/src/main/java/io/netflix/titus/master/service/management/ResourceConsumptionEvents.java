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

package io.netflix.titus.master.service.management;

/**
 */
public class ResourceConsumptionEvents {
    public abstract static class ResourceConsumptionEvent {
        private final String capacityGroup;
        private final long timestamp;

        public ResourceConsumptionEvent(String capacityGroup, long timestamp) {
            this.capacityGroup = capacityGroup;
            this.timestamp = timestamp;
        }

        public String getCapacityGroup() {
            return capacityGroup;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Objects of this class represent a resource utilization for a capacity category at a point in time.
     */
    public static class CapacityGroupAllocationEvent extends ResourceConsumptionEvent {

        private final CompositeResourceConsumption capacityGroupConsumption;

        public CapacityGroupAllocationEvent(String capacityGroup,
                                            long timestamp,
                                            CompositeResourceConsumption capacityGroupConsumption) {
            super(capacityGroup, timestamp);
            this.capacityGroupConsumption = capacityGroupConsumption;
        }

        public CompositeResourceConsumption getCapacityGroupConsumption() {
            return capacityGroupConsumption;
        }
    }

    public static class CapacityGroupUndefinedEvent extends ResourceConsumptionEvent {
        public CapacityGroupUndefinedEvent(String capacityGroup,
                                           long timestamp) {
            super(capacityGroup, timestamp);
        }
    }

    public static class CapacityGroupRemovedEvent extends ResourceConsumptionEvent {
        public CapacityGroupRemovedEvent(String capacityGroup,
                                         long timestamp) {
            super(capacityGroup, timestamp);
        }
    }
}
