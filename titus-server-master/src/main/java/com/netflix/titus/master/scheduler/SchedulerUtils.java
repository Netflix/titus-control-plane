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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTracker;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
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

    public static String getZoneId(VirtualMachineCurrentState targetVM, String zoneAttributeName) {
        Map<String, Protos.Attribute> attributeMap = targetVM.getCurrAvailableResources().getAttributeMap();
        if (attributeMap == null) {
            return null;
        }

        Protos.Attribute attributeValue = attributeMap.get(zoneAttributeName);
        if (attributeValue == null) {
            return null;
        }

        if (!attributeValue.hasText()) {
            return null;
        }
        String value = attributeValue.getText().getValue();
        return value == null || value.isEmpty() ? null : value;
    }

    public static Map<String, Integer> groupCurrentlyAssignedTasksByZoneId(String jobId, Collection<TaskTracker.ActiveTask> tasksCurrentlyAssigned, String zoneAttributeName) {
        Map<String, Integer> result = new HashMap<>();

        for (TaskTracker.ActiveTask activeTask : tasksCurrentlyAssigned) {
            TaskRequest request = activeTask.getTaskRequest();
            if (request instanceof V3QueueableTask) {
                String requestJobId = ((V3QueueableTask) request).getJob().getId();
                if (jobId.equals(requestJobId)) {
                    Map<String, Protos.Attribute> attributeMap = activeTask.getTotalLease().getAttributeMap();
                    if (attributeMap != null) {
                        Protos.Attribute zoneIdAttribute = attributeMap.get(zoneAttributeName);
                        if (zoneIdAttribute != null) {
                            String zoneId = zoneIdAttribute.getText().getValue();
                            if (StringExt.isNotEmpty(zoneId)) {
                                result.put(zoneId, result.getOrDefault(zoneId, 0));
                            }
                        }
                    }
                }
            }
        }

        return result;
    }
}
