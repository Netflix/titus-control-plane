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

package com.netflix.titus.master.service.management.internal;

import java.util.Collections;

import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.master.service.management.CompositeResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupAllocationEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupRemovedEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupUndefinedEvent;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ResourceConsumptionLogTest {

    @Test
    public void testAllocationEventLogging() throws Exception {
        CompositeResourceConsumption consumption = new CompositeResourceConsumption(
                "myCapacityGroup",
                ResourceConsumption.ConsumptionLevel.CapacityGroup,
                new ResourceDimension(1, 0, 1, 10, 10), // actual
                new ResourceDimension(2, 0, 2, 20, 20), // max
                new ResourceDimension(3, 0, 3, 30, 30), // limit
                Collections.singletonMap("attrKey", "attrValue"),
                Collections.emptyMap(),
                false
        );
        CapacityGroupAllocationEvent event = new CapacityGroupAllocationEvent(
                "myCapacityGroup",
                System.currentTimeMillis(),
                consumption
        );
        String result = ResourceConsumptionLog.doLog(event);

        String expected = "Resource consumption change: group=myCapacityGroup [below limit] actual=[cpu=1.0, memoryMB=1, diskMB=10, networkMbs=10], max=[cpu=2.0, memoryMB=2, diskMB=20, networkMbs=20], limit=[cpu=3.0, memoryMB=3, diskMB=30, networkMbs=30], attrs={attrKey=attrValue}";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testGroupUndefinedEvent() throws Exception {
        CapacityGroupUndefinedEvent event = new CapacityGroupUndefinedEvent("myCapacityGroup", System.currentTimeMillis());
        String result = ResourceConsumptionLog.doLog(event);

        String expected = "Capacity group not defined: group=myCapacityGroup";
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testGroupRemovedEvent() throws Exception {
        CapacityGroupRemovedEvent event = new CapacityGroupRemovedEvent("myCapacityGroup", System.currentTimeMillis());
        String result = ResourceConsumptionLog.doLog(event);

        String expected = "Capacity group no longer defined: group=myCapacityGroup";
        assertThat(result).isEqualTo(expected);
    }
}