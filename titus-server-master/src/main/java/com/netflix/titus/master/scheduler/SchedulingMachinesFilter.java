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

package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.AssignableVirtualMachine;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;

import static com.netflix.titus.master.scheduler.SchedulerUtils.isLaunchingLessThanNumberOfTasks;

@Singleton
public class SchedulingMachinesFilter {

    private final SchedulerConfiguration schedulerConfiguration;
    private final AgentManagementService agentManagementService;
    private final AgentStatusMonitor agentStatusMonitor;
    private final V3JobOperations v3JobOperations;

    @Inject
    public SchedulingMachinesFilter(SchedulerConfiguration schedulerConfiguration,
                                    AgentManagementService agentManagementService,
                                    AgentStatusMonitor agentStatusMonitor,
                                    V3JobOperations v3JobOperations) {
        this.schedulerConfiguration = schedulerConfiguration;
        this.agentManagementService = agentManagementService;
        this.agentStatusMonitor = agentStatusMonitor;
        this.v3JobOperations = v3JobOperations;
    }

    public List<AssignableVirtualMachine> filter(List<AssignableVirtualMachine> machines) {
        if (schedulerConfiguration.isSchedulingMachinesFilterEnabled()) {
            Map<String, Task> tasksById = new HashMap<>();
            for (Task task : v3JobOperations.getTasks()) {
                tasksById.put(task.getId(), task);
            }

            int maxLaunchingTasksPerMachine = schedulerConfiguration.getMaxLaunchingTasksPerMachine();
            List<AssignableVirtualMachine> filteredMachines = new ArrayList<>();

            for (AssignableVirtualMachine machine : machines) {
                VirtualMachineCurrentState vmCurrentState = machine.getVmCurrentState();
                Optional<AgentInstance> instanceOpt = SchedulerUtils.findInstance(agentManagementService,
                        schedulerConfiguration.getInstanceAttributeName(), vmCurrentState);

                if (!instanceOpt.isPresent()) {
                    continue;
                }

                AgentInstance instance = instanceOpt.get();

                if (!isInstanceSchedulable(instance) ||
                        isInstanceLaunchingTooManyTasks(tasksById, vmCurrentState, maxLaunchingTasksPerMachine)) {
                    continue;
                }

                String instanceGroupId = instance.getInstanceGroupId();

                Optional<AgentInstanceGroup> instanceGroupOpt = agentManagementService.findInstanceGroup(instanceGroupId);
                if (!instanceGroupOpt.isPresent()) {
                    continue;
                }

                AgentInstanceGroup instanceGroup = instanceGroupOpt.get();

                if (!isInstanceGroupSchedulable(instanceGroup)) {
                    continue;
                }

                filteredMachines.add(machine);
            }
            return filteredMachines;
        }
        return machines;
    }

    private boolean isInstanceGroupSchedulable(AgentInstanceGroup instanceGroup) {
        InstanceGroupLifecycleState state = instanceGroup.getLifecycleStatus().getState();
        return (state == InstanceGroupLifecycleState.Active || state == InstanceGroupLifecycleState.PhasedOut) &&
                isSchedulable(instanceGroup.getAttributes());
    }

    private boolean isInstanceSchedulable(AgentInstance instance) {
        InstanceLifecycleState state = instance.getLifecycleStatus().getState();
        return state == InstanceLifecycleState.Started &&
                isSchedulable(instance.getAttributes()) &&
                agentStatusMonitor.isHealthy(instance.getId());
    }

    private boolean isSchedulable(Map<String, String> attributes) {
        boolean systemNoPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.SYSTEM_NO_PLACEMENT));
        if (systemNoPlacement) {
            return false;
        }

        boolean noPlacement = Boolean.parseBoolean(attributes.get(SchedulerAttributes.NO_PLACEMENT));
        return !noPlacement;
    }

    private boolean isInstanceLaunchingTooManyTasks(Map<String, Task> tasksById,
                                                    VirtualMachineCurrentState vmCurrentState,
                                                    int maxLaunchingTasksPerMachine) {
        return !isLaunchingLessThanNumberOfTasks(tasksById, vmCurrentState, maxLaunchingTasksPerMachine);
    }
}
