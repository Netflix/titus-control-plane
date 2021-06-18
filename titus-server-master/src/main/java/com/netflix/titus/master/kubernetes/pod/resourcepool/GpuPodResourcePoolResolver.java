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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GpuPodResourcePoolResolver implements PodResourcePoolResolver {

    private static final Logger logger = LoggerFactory.getLogger(GpuPodResourcePoolResolver.class);

    private final KubePodConfiguration configuration;

    private final Lock lock = new ReentrantLock();
    private volatile String lastParsedValue;
    private volatile Set<String> gpuResourcePoolNames = Collections.emptySet();
    private volatile List<ResourcePoolAssignment> gpuResourcePoolAssignments = Collections.emptyList();
    private final ApplicationSlaManagementService capacityGroupService;

    public GpuPodResourcePoolResolver(KubePodConfiguration configuration, ApplicationSlaManagementService capacityGroupService) {
        this.configuration = configuration;
        this.capacityGroupService = capacityGroupService;
        // Call early to initialize before the first usage.
        getCurrent();
    }

    @Override
    public List<ResourcePoolAssignment> resolve(Job<?> job, Task task) {
        if (job.getJobDescriptor().getContainer().getContainerResources().getGpu() <= 0) {
            return Collections.emptyList();
        }
        // If capacity group is already configured with a GPU resource pool, use just that rather than a list of GPU resource pools
        // as defined in the GpuResourcePoolNames fast property
        ApplicationSLA capacityGroup = JobManagerUtil.getCapacityGroupDescriptor(job.getJobDescriptor(), capacityGroupService);
        if (capacityGroup != null && StringExt.isNotEmpty(capacityGroup.getResourcePool()) &&
                this.gpuResourcePoolNames.contains(capacityGroup.getResourcePool())) {
            String resourcePoolName = capacityGroup.getResourcePool();
            return Collections.singletonList(ResourcePoolAssignment.newBuilder()
                    .withResourcePoolName(resourcePoolName)
                    .withRule("GPU task assigned to application Capacity Group " + resourcePoolName)
                    .build());
        }
        return getCurrent();
    }

    private List<ResourcePoolAssignment> getCurrent() {
        if (!lock.tryLock()) {
            return gpuResourcePoolAssignments;
        }

        try {
            String currentValue = configuration.getGpuResourcePoolNames();
            if (!Objects.equals(lastParsedValue, currentValue)) {
                this.lastParsedValue = currentValue;
                this.gpuResourcePoolNames = StringExt.splitByCommaIntoSet(currentValue);
                List<ResourcePoolAssignment> assignments = new ArrayList<>();
                for (String gpuResourcePoolName : this.gpuResourcePoolNames) {
                    assignments.add(ResourcePoolAssignment.newBuilder()
                            .withResourcePoolName(gpuResourcePoolName)
                            .withRule("GPU resources pool assignment")
                            .build());
                }
                this.gpuResourcePoolAssignments = assignments;
            }
        } catch (Exception e) {
            logger.error("Cannot parse GPU resource pool", e);
        } finally {
            lock.unlock();
        }
        return gpuResourcePoolAssignments;
    }
}
