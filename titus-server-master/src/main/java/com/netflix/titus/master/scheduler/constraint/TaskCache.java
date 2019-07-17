/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.vpc.SignedIpAddressAllocation;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Helper class that aggregates task data by multiple criteria used by Fenzo constraint/fitness evaluators.
 */
@Singleton
public class TaskCache {

    private final V3JobOperations v3JobOperations;
    private final AtomicReference<TaskCacheValue> currentCacheValue;

    @Inject
    public TaskCache(V3JobOperations v3JobOperations) {
        this.v3JobOperations = v3JobOperations;
        this.currentCacheValue = new AtomicReference<>();
    }

    public void prepare() {
        currentCacheValue.set(new TaskCacheValue());
    }

    public Map<String, Integer> getTasksByZoneIdCounters(String jobId) {
        return currentCacheValue.get().getTasksByZoneIdCounters(jobId);
    }

    // Returns a task ID if there is a task assigned to the provided IP allocation
    public Optional<String> getTaskByIpAllocationId(String ipAllocationId) {
        return Optional.ofNullable(currentCacheValue.get().assignedIpAllocations.getOrDefault(ipAllocationId, null));
    }

    // Updates the cache to reflect assignment of an IP allocation to a task
    public void addTaskIpAllocation(String ipAllocationId, String taskId) {
        currentCacheValue.get().assignedIpAllocations.put(ipAllocationId, taskId);
    }

    public Optional<String> getZoneIdByIpAllocationId(String ipAllocationId) {
        return Optional.ofNullable(currentCacheValue.get().ipAllocationIdToZoneId.getOrDefault(ipAllocationId, null));
    }

    private class TaskCacheValue {

        private final Map<String, Map<String, Integer>> zoneBalanceCountersByJobId;

        // This map contains currently assigned IP allocations, Map<IP Allocation ID, Task ID>
        private final Map<String, String> assignedIpAllocations;

        // Maps an IP allocation ID to the zone it exists in, Map<IP Allocation ID, Zone ID>
        private final Map<String, String> ipAllocationIdToZoneId;

        private TaskCacheValue() {
            List<Pair<Job, List<Task>>> jobsAndTasks = v3JobOperations.getJobsAndTasks();
            this.assignedIpAllocations = new ConcurrentHashMap<>();
            this.zoneBalanceCountersByJobId = new HashMap<>();
            this.ipAllocationIdToZoneId = new HashMap<>();
            buildTaskCacheInfo(jobsAndTasks);
        }

        private Map<String, Integer> getTasksByZoneIdCounters(String jobId) {
            return zoneBalanceCountersByJobId.getOrDefault(jobId, Collections.emptyMap());
        }

        private void buildTaskCacheInfo(List<Pair<Job, List<Task>>> jobsAndTasks) {
            for (Pair<Job, List<Task>> jobAndTask : jobsAndTasks) {
                Map<String, Integer> jobZoneBalancing = new HashMap<>();
                for (Task task : jobAndTask.getRight()) {
                    String zoneId = getZoneId(task);
                    if (zoneId != null) {
                        jobZoneBalancing.put(zoneId, jobZoneBalancing.getOrDefault(zoneId, 0) + 1);
                    }

                    getIpAllocationId(task).map(ipAllocationId -> {
                        ipAllocationIdToZoneId.put(
                                ipAllocationId,
                                getIpAllocationZone(ipAllocationId, jobAndTask.getLeft().getJobDescriptor()).orElse(""));
                        if (TaskState.isRunning(task.getStatus().getState())) {
                            assignedIpAllocations.put(ipAllocationId, task.getId());
                        }
                        return null;
                    });
                }
                zoneBalanceCountersByJobId.put(jobAndTask.getLeft().getId(), jobZoneBalancing);
            }
        }

        private String getZoneId(Task task) {
            return task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE);
        }

        private Optional<String> getIpAllocationId(Task task) {
            return Optional.ofNullable(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_IP_ALLOCATION_ID));
        }

        private Optional<String> getIpAllocationZone(String ipAllocationId, JobDescriptor<?> jobDescriptor) {
            for (SignedIpAddressAllocation signedIpAddressAllocation : jobDescriptor.getContainer().getContainerResources().getSignedIpAddressAllocations()) {
                if (signedIpAddressAllocation.getIpAddressAllocation().getAllocationId().equals(ipAllocationId)) {
                    return Optional.of(signedIpAddressAllocation.getIpAddressAllocation().getIpAddressLocation().getAvailabilityZone());
                }
            }
            return Optional.empty();
        }
    }
}