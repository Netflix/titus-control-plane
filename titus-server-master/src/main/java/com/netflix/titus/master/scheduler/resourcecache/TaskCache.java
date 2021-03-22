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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.vpc.IpAddressAllocationUtils;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Helper class that aggregates task data by multiple criteria used by Fenzo constraint/fitness evaluators.
 */
@Singleton
public class TaskCache {

    private final TitusRuntime titusRuntime;
    private final V3JobOperations v3JobOperations;
    private final AtomicReference<TaskCacheValue> currentCacheValue;

    @Inject
    public TaskCache(TitusRuntime titusRuntime, V3JobOperations v3JobOperations) {
        this.titusRuntime = titusRuntime;
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
        return Optional.ofNullable(currentCacheValue.get().assignedIpAllocations.get(ipAllocationId));
    }

    // Updates the cache to reflect assignment of an IP allocation to a task
    public void addTaskIpAllocation(String ipAllocationId, String taskId) {
        currentCacheValue.get().assignedIpAllocations.put(ipAllocationId, taskId);
    }

    public Optional<String> getZoneIdByIpAllocationId(String ipAllocationId) {
        return Optional.ofNullable(currentCacheValue.get().ipAllocationIdToZoneId.get(ipAllocationId));
    }

    public void addOpportunisticCpuAllocation(OpportunisticCpuAllocation allocation) {
        CollectionsExt.multiMapAddValue(currentCacheValue.get().assignedOpportunisticCpus, allocation.getAgentId(), allocation);
    }

    public int getOpportunisticCpusAllocated(String agentId) {
        return currentCacheValue.get().assignedOpportunisticCpus.getOrDefault(agentId, Collections.emptyList()).stream()
                .mapToInt(OpportunisticCpuAllocation::getCpuCount)
                .sum();
    }

    private class TaskCacheValue {

        private final Map<String, Map<String, Integer>> zoneBalanceCountersByJobId;

        // This map contains currently assigned IP allocations, Map<IP Allocation ID, Task ID>
        private final ConcurrentMap<String, String> assignedIpAllocations;

        // Maps an IP allocation ID to the zone it exists in, Map<IP Allocation ID, Zone ID>
        private final Map<String, String> ipAllocationIdToZoneId;

        // agentId -> all tasks that were allocated with
        private final ConcurrentMap<String, List<OpportunisticCpuAllocation>> assignedOpportunisticCpus;

        private TaskCacheValue() {
            List<Pair<Job, List<Task>>> jobsAndTasks = v3JobOperations.getJobsAndTasks();
            this.assignedIpAllocations = new ConcurrentHashMap<>();
            this.zoneBalanceCountersByJobId = new HashMap<>();
            this.ipAllocationIdToZoneId = new HashMap<>();
            this.assignedOpportunisticCpus = new ConcurrentHashMap<>();
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

                    // Get an IP allocation ID that has been assigned to this task. If present,
                    // check if the task is running and if so, mark the IP allocation as in use.
                    // In addition, resolve the IP allocation ID's zone ID and cache that as well.
                    IpAddressAllocationUtils.getIpAllocationId(task).ifPresent(ipAllocationId -> {
                        if (TaskState.isRunning(task.getStatus().getState())) {
                            assignedIpAllocations.put(ipAllocationId, task.getId());
                        }
                        IpAddressAllocationUtils.getIpAllocationZoneForId(ipAllocationId, jobAndTask.getLeft().getJobDescriptor(), codeInvariants()).ifPresent(zoneIdForIpAllocation ->
                                ipAllocationIdToZoneId.put(ipAllocationId, zoneIdForIpAllocation));
                    });

                    getOpportunisticCpuAllocation(task).ifPresent(allocation -> {
                        if (TaskState.isRunning(task.getStatus().getState())) {
                            CollectionsExt.multiMapAddValue(assignedOpportunisticCpus, allocation.getAgentId(), allocation);
                        }
                    });
                }
                zoneBalanceCountersByJobId.put(jobAndTask.getLeft().getId(), jobZoneBalancing);
            }
        }

        private Optional<OpportunisticCpuAllocation> getOpportunisticCpuAllocation(Task task) {
            Optional<String> allocationIdOpt = getOpportunisticCpuAllocationId(task);
            Optional<OpportunisticCpuAllocation> allocationOpt = allocationIdOpt
                    .map(allocationId -> OpportunisticCpuAllocation.newBuilder().withAllocationId(allocationId))
                    .flatMap(builder -> JobFunctions.getOpportunisticCpuCount(task).map(builder::withCpuCount))
                    .flatMap(builder -> getAgentId(task).map(builder::withAgentId))
                    .map(builder -> builder.withTaskId(task.getId()).build());

            if (allocationIdOpt.isPresent() && !allocationOpt.isPresent()) {
                codeInvariants().inconsistent("Task %s is allocated opportunistic CPU, but is missing extra required information",
                        task.getId());
            }
            return allocationOpt;
        }
    }

    private CodeInvariants codeInvariants() {
        return titusRuntime.getCodeInvariants();
    }

    private static Optional<String> getAgentId(Task task) {
        return Optional.ofNullable(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID));
    }

    private static Optional<String> getOpportunisticCpuAllocationId(Task task) {
        return Optional.ofNullable(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION));
    }

    private static String getZoneId(Task task) {
        return task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE);
    }

}