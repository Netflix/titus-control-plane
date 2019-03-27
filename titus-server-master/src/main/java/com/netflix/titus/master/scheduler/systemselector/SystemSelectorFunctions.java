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

package com.netflix.titus.master.scheduler.systemselector;

import java.util.HashMap;
import java.util.Map;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;

import static com.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.getAgentAttributeValue;

public class SystemSelectorFunctions {

    //TODO I need to create a context with fake job, task, instance group, instance
    //TODO I need to convert v2 models into v3 so that all selectors can be written with v3 structure

    private static final String JOB = "job";
    private static final String TASK = "task";
    private static final String INSTANCE_GROUP = "instanceGroup";
    private static final String INSTANCE = "instance";

    public static Map<String, Object> createContext(TaskRequest taskRequest,
                                                    VirtualMachineCurrentState targetVM,
                                                    AgentManagementService agentManagementService,
                                                    SchedulerConfiguration schedulerConfiguration) {
        Map<String, Object> context = new HashMap<>();
        setJobAndTask(context, taskRequest);
        setInstanceGroupAndInstance(context, targetVM, agentManagementService, schedulerConfiguration);
        return context;
    }

    private static void setJobAndTask(Map<String, Object> context, TaskRequest taskRequest) {
        if (taskRequest instanceof V3QueueableTask) {
            V3QueueableTask v3QueueableTask = (V3QueueableTask) taskRequest;
            Job job = v3QueueableTask.getJob();
            Task task = v3QueueableTask.getTask();
            context.put(JOB, job);
            context.put(TASK, task);
        }
    }

    private static void setInstanceGroupAndInstance(Map<String, Object> context,
                                                    VirtualMachineCurrentState targetVM,
                                                    AgentManagementService agentManagementService,
                                                    SchedulerConfiguration schedulerConfiguration) {
        String instanceId = getAgentAttributeValue(targetVM, schedulerConfiguration.getInstanceAttributeName());
        try {
            AgentInstance instance = agentManagementService.getAgentInstance(instanceId);
            AgentInstanceGroup instanceGroup = agentManagementService.getInstanceGroup(instance.getInstanceGroupId());
            context.put(INSTANCE_GROUP, instanceGroup);
            context.put(INSTANCE, instance);
        } catch (Exception ignored) {
            context.put(INSTANCE_GROUP, AgentInstanceGroup.newBuilder());
            context.put(INSTANCE, AgentInstance.newBuilder());
        }
    }
}
