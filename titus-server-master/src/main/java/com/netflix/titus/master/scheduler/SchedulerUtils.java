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

package com.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QueuableTask;
import org.apache.mesos.Protos;

public class SchedulerUtils {

    public static boolean hasGpuRequest(ScheduledRequest request) {
        return hasGpuRequest((QueuableTask) request);
    }

    public static boolean hasGpuRequest(QueuableTask task) {
        return task != null && task.getScalarRequests() != null &&
                task.getScalarRequests().get("gpu") != null &&
                task.getScalarRequests().get("gpu") >= 1.0;
    }

    public static Optional<String> getInstanceGroupName(String instanceGroupAttributeName, VirtualMachineLease lease) {
        Map<String, Protos.Attribute> attributeMap = lease.getAttributeMap();
        if (attributeMap != null) {
            final Protos.Attribute attribute = attributeMap.get(instanceGroupAttributeName);
            if (attribute != null && attribute.hasText()) {
                return Optional.of(attribute.getText().getValue());
            }
        }
        return Optional.empty();
    }

    public static String getAttributeValue(VirtualMachineCurrentState targetVM, String attributeName) {
        Protos.Attribute attribute = targetVM.getCurrAvailableResources().getAttributeMap().get(attributeName);
        return Strings.nullToEmpty(attribute.getText().getValue());
    }
}
