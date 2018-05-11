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
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import org.apache.mesos.Protos;

public class SchedulerUtils {

    public static boolean hasGpuRequest(QueuableTask task) {
        return task != null && task.getScalarRequests() != null &&
                task.getScalarRequests().get("gpu") != null &&
                task.getScalarRequests().get("gpu") >= 1.0;
    }

    public static String getAttributeValueOrEmptyString(Map<String, Protos.Attribute> attributeMap, String attributeName) {
        if (attributeMap == null) {
            return "";
        }

        Protos.Attribute attributeValue = attributeMap.get(attributeName);
        if (attributeValue == null) {
            return "";
        }

        if (!attributeValue.hasText()) {
            return "";
        }
        return Strings.nullToEmpty(attributeValue.getText().getValue());
    }

    public static String getAttributeValueOrEmptyString(VirtualMachineCurrentState targetVM, String attributeName) {
        return getAttributeValueOrEmptyString(targetVM.getCurrAvailableResources().getAttributeMap(), attributeName);
    }

    public static Optional<String> getInstanceGroupName(String instanceGroupAttributeName, VirtualMachineLease lease) {
        String name = getAttributeValueOrEmptyString(lease.getAttributeMap(), instanceGroupAttributeName);
        return name.isEmpty() ? Optional.empty() : Optional.of(name);
    }

    public static Map<String, Integer> groupCurrentlyAssignedTasksByZoneId(String jobId, Collection<TaskTracker.ActiveTask> tasksCurrentlyAssigned, String zoneAttributeName) {
        Map<String, Integer> result = new HashMap<>();

        for (TaskTracker.ActiveTask activeTask : tasksCurrentlyAssigned) {
            TaskRequest request = activeTask.getTaskRequest();
            if (request instanceof V3QueueableTask) {
                String requestJobId = ((V3QueueableTask) request).getJob().getId();
                if (jobId.equals(requestJobId)) {
                    String zoneId = getAttributeValueOrEmptyString(activeTask.getTotalLease().getAttributeMap(), zoneAttributeName);
                    if (!zoneId.isEmpty()) {
                        result.put(zoneId, result.getOrDefault(zoneId, 0) + 1);
                    }
                }
            }
        }

        return result;
    }
}
