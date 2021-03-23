/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job.vpc;

import java.util.Optional;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.code.CodeInvariants;

/**
 * Helper utilities for processing Static IP related info.
 */
public class IpAddressAllocationUtils {

    public static Optional<String> getIpAllocationId(Task task) {
        return Optional.ofNullable(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID));
    }

    public static Optional<String> getIpAllocationZoneForId(String ipAllocationId, JobDescriptor<?> jobDescriptor, CodeInvariants codeInvariants) {
        for (SignedIpAddressAllocation signedIpAddressAllocation : jobDescriptor.getContainer().getContainerResources().getSignedIpAddressAllocations()) {
            if (signedIpAddressAllocation.getIpAddressAllocation().getAllocationId().equals(ipAllocationId)) {
                return Optional.of(signedIpAddressAllocation.getIpAddressAllocation().getIpAddressLocation().getAvailabilityZone());
            }
        }

        codeInvariants.inconsistent("Unable to find zone for IP allocation ID {} in job allocations {}",
                ipAllocationId, jobDescriptor.getContainer().getContainerResources().getSignedIpAddressAllocations());
        return Optional.empty();
    }

    /**
     * Returns the zone if the task has an assigned IP address allocation.
     */
    public static Optional<String> getIpAllocationZoneForTask(JobDescriptor<?> jobDescriptor, Task task, CodeInvariants codeInvariants) {
        return getIpAllocationId(task)
                .flatMap(ipAllocationId -> getIpAllocationZoneForId(ipAllocationId, jobDescriptor, codeInvariants));
    }
}
