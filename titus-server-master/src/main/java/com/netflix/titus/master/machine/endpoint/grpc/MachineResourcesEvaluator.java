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

package com.netflix.titus.master.machine.endpoint.grpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.v4.CategorizedMachineResources;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineResources;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;

@Singleton
public class MachineResourcesEvaluator {

    private static final String MACHINE_ATTRIBUTE_CELL_ID = "titus.cell";
    
    public static String MACHINE_RESOURCE_PHYSICAL = "physical";

    public static String MACHINE_RESOURCE_OPPORTUNISTIC_ALLOCATED_UNUSED = "allocatedUnused";

    private final MasterConfiguration masterConfiguration;
    private final ReadOnlyAgentOperations agentOperations;
    private final ReadOnlyJobOperations jobOperations;
    private final OpportunisticCpuAvailabilityProvider opportunisticCpuAvailabilityProvider;

    @Inject
    public MachineResourcesEvaluator(MasterConfiguration masterConfiguration,
                                     ReadOnlyAgentOperations agentOperations,
                                     ReadOnlyJobOperations jobOperations,
                                     OpportunisticCpuAvailabilityProvider opportunisticCpuAvailabilityProvider) {
        this.masterConfiguration = masterConfiguration;
        this.agentOperations = agentOperations;
        this.jobOperations = jobOperations;
        this.opportunisticCpuAvailabilityProvider = opportunisticCpuAvailabilityProvider;
    }

    public Map<String, Machine> evaluate() {
        List<Pair<AgentInstanceGroup, List<AgentInstance>>> instanceGroupAndInstances = agentOperations.findAgentInstances(p -> true);

        Map<String, AgentInstanceGroup> instanceGroupMap = instanceGroupAndInstances.stream()
                .map(Pair::getLeft)
                .collect(Collectors.toMap(AgentInstanceGroup::getId, g -> g));

        Map<String, AgentInstance> instanceMap = instanceGroupAndInstances.stream()
                .flatMap(p -> p.getRight().stream())
                .collect(Collectors.toMap(AgentInstance::getId, i -> i));

        Map<String, OpportunisticCpuAvailability> opportunistic = opportunisticCpuAvailabilityProvider.getOpportunisticCpus();

        Map<String, Map<String, JobTaskCounter>> tasksByInstanceId = groupJobsByInstanceIds(instanceMap);

        Map<String, Machine> machines = new HashMap<>();
        instanceMap.forEach((instanceId, instance) -> {
            AgentInstanceGroup instanceGroup = instanceGroupMap.get(instance.getInstanceGroupId());
            if (instanceGroup != null) {
                Machine machine = toMachine(
                        instanceGroup,
                        instance,
                        opportunistic.get(instanceId),
                        tasksByInstanceId.getOrDefault(instanceId, Collections.emptyMap())
                );
                machines.put(instanceId, machine);
            }
        });
        return machines;
    }

    private Map<String, Map<String, JobTaskCounter>> groupJobsByInstanceIds(Map<String, AgentInstance> instanceMap) {
        Map<String, Map<String, JobTaskCounter>> result = new HashMap<>();

        for (Pair<Job, List<Task>> jobAndTasks : jobOperations.getJobsAndTasks()) {
            Job job = jobAndTasks.getLeft();
            for (Task task : jobAndTasks.getRight()) {
                if (TaskState.isRunning(task.getStatus().getState())) {
                    String taskInstanceId = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID);
                    if (taskInstanceId != null && instanceMap.containsKey(taskInstanceId)) {
                        Map<String, JobTaskCounter> instanceJobCounters = result.computeIfAbsent(taskInstanceId, id -> new HashMap<>());
                        instanceJobCounters.computeIfAbsent(job.getId(), id -> new JobTaskCounter(job)).increment();
                    }
                }
            }
        }

        return result;
    }

    private Machine toMachine(AgentInstanceGroup instanceGroup,
                              AgentInstance agentInstance,
                              OpportunisticCpuAvailability opportunisticCpuAvailability,
                              Map<String, JobTaskCounter> counters) {
        int allocatedCpu = 0;
        int allocatedMemoryMb = 0;
        int allocatedDiskMb = 0;
        int allocatedNetworkMbps = 0;

        for (JobTaskCounter jobTaskCounter : counters.values()) {
            ContainerResources taskResources = jobTaskCounter.getJob().getJobDescriptor().getContainer().getContainerResources();
            int counter = jobTaskCounter.getTaskCounter();
            allocatedCpu += counter * taskResources.getCpu();
            allocatedMemoryMb += counter * taskResources.getMemoryMB();
            allocatedDiskMb += counter * taskResources.getDiskMB();
            allocatedNetworkMbps += counter * taskResources.getNetworkMbps();
        }

        CategorizedMachineResources.Builder opportunisticBuilder = CategorizedMachineResources.newBuilder()
                .setCategory(MACHINE_RESOURCE_OPPORTUNISTIC_ALLOCATED_UNUSED);
        if (opportunisticCpuAvailability != null) {
            opportunisticBuilder
                    .setMachineResources(MachineResources.newBuilder()
                            .setCpu(opportunisticCpuAvailability.getCount())
                    )
                    .putLabels("allocationId", "" + opportunisticCpuAvailability.getAllocationId())
                    .putLabels("expiresAtTimestamp", "" + opportunisticCpuAvailability.getExpiresAt().toEpochMilli());
        }

        return Machine.newBuilder()
                .setId(agentInstance.getId())
                .putAllAttributes(instanceGroup.getAttributes())
                .putAllAttributes(agentInstance.getAttributes())
                .putAttributes(MACHINE_ATTRIBUTE_CELL_ID, masterConfiguration.getCellName())
                .setAllocatedResources(MachineResources.newBuilder()
                        .setCpu(allocatedCpu)
                        .setMemoryMB(allocatedMemoryMb)
                        .setDiskMB(allocatedDiskMb)
                        .setNetworkMbps(allocatedNetworkMbps)
                        .build()
                )
                .addInstanceResources(CategorizedMachineResources.newBuilder()
                        .setCategory(MACHINE_RESOURCE_PHYSICAL)
                        .setMachineResources(MachineResources.newBuilder()
                                .setCpu((int) instanceGroup.getResourceDimension().getCpu())
                                .setMemoryMB((int) instanceGroup.getResourceDimension().getMemoryMB())
                                .setDiskMB((int) instanceGroup.getResourceDimension().getDiskMB())
                                .setNetworkMbps((int) instanceGroup.getResourceDimension().getNetworkMbs())
                        )
                )
                .addInstanceResources(opportunisticBuilder)
                .build();
    }

    private static class JobTaskCounter {

        private final Job<?> job;
        private int taskCounter;

        private JobTaskCounter(Job<?> job) {
            this.job = job;
        }

        private Job<?> getJob() {
            return job;
        }

        private int getTaskCounter() {
            return taskCounter;
        }

        private void increment() {
            taskCounter++;
        }
    }
}
