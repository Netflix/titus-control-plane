/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;

public class FenzoPodResourcePoolResolver implements PodResourcePoolResolver {

    private static final ResourcePoolAssignment FENZO_FLEX = ResourcePoolAssignment.newBuilder()
            .withResourcePoolName(PodResourcePoolResolvers.RESOURCE_POOL_FENZO_FLEX)
            .withRule("Fenzo task assigned to fenzo flex resource pool")
            .build();

    private static final ResourcePoolAssignment FENZO_CRITICAL = ResourcePoolAssignment.newBuilder()
            .withResourcePoolName(PodResourcePoolResolvers.RESOURCE_POOL_FENZO_CRITICAL)
            .withRule("Fenzo task assigned to fenzo critical resource pool")
            .build();

    private final ApplicationSlaManagementService capacityGroupService;

    public FenzoPodResourcePoolResolver(ApplicationSlaManagementService capacityGroupService) {
        this.capacityGroupService = capacityGroupService;
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job, Task task) {
        if (JobFunctions.isOwnedByKubeScheduler(task)) {
            return Collections.emptyList();
        }

        ApplicationSLA capacityGroup = JobManagerUtil.getCapacityGroupDescriptor(job.getJobDescriptor(), capacityGroupService);
        if (capacityGroup == null || capacityGroup.getTier() != Tier.Critical) {
            return Collections.singletonList(FENZO_FLEX);
        }
        return Collections.singletonList(FENZO_CRITICAL);
    }
}
