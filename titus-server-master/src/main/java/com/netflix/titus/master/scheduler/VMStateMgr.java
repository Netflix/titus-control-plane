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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;

import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

class VMStateMgr {

    static List<VirtualMachineCurrentState> getInactiveVMs(String instanceIdAttributeName,
                                                           AgentManagementService agentManagementService,
                                                           List<VirtualMachineCurrentState> currentStates) {
        if (isNullOrEmpty(currentStates)) {
            return Collections.emptyList();
        }

        List<VirtualMachineCurrentState> inactiveVMs = new ArrayList<>();
        for (VirtualMachineCurrentState currentState : currentStates) {
            final VirtualMachineLease lease = currentState.getCurrAvailableResources();
            if (lease != null) {
                final Collection<TaskRequest> runningTasks = currentState.getRunningTasks();
                if (!isNullOrEmpty(runningTasks)) {
                    SchedulerUtils.findInstance(agentManagementService, instanceIdAttributeName, currentState)
                            .map(AgentInstance::getInstanceGroupId)
                            .flatMap(agentManagementService::findInstanceGroup)
                            .ifPresent(instanceGroup -> {
                                InstanceGroupLifecycleState state = instanceGroup.getLifecycleStatus().getState();
                                if (state == InstanceGroupLifecycleState.Inactive || state == InstanceGroupLifecycleState.Removable) {
                                    inactiveVMs.add(currentState);
                                }
                            });
                }
            }
        }
        return inactiveVMs;
    }
}
