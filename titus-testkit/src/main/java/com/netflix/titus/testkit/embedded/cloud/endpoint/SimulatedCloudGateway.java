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

package com.netflix.titus.testkit.embedded.cloud.endpoint;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedComputeResources;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstance;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup.Capacity;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTask;
import com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import org.apache.mesos.Protos;

import static com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor.awsInstanceGroup;

@Singleton
public class SimulatedCloudGateway {

    private final SimulatedCloud simulatedCloud;

    @Inject
    public SimulatedCloudGateway(SimulatedCloud simulatedCloud) {
        this.simulatedCloud = simulatedCloud;
    }

    public List<SimulatedInstanceGroup> getAllInstanceGroups() {
        return simulatedCloud.getAgentInstanceGroups().stream().map(this::toSimulatedInstanceGroup).collect(Collectors.toList());
    }

    public List<SimulatedInstanceGroup> getInstanceGroups(Set<String> instanceGroupIds) {
        return simulatedCloud.getAgentInstanceGroups().stream()
                .filter(g -> instanceGroupIds.contains(g.getName()))
                .map(this::toSimulatedInstanceGroup)
                .collect(Collectors.toList());
    }

    public void addInstanceGroup(String id, AwsInstanceType instanceType, int min, int desired, int max) {
        simulatedCloud.createAgentInstanceGroups(SimulatedAgentGroupDescriptor.awsInstanceGroup(id, instanceType, min, desired, max));
    }

    public List<SimulatedInstance> getInstances(String instanceGroupId) {
        return simulatedCloud.getAgentInstanceGroup(instanceGroupId).getAgents().stream()
                .map(this::toSimulatedInstance)
                .collect(Collectors.toList());
    }

    public SimulatedInstance getInstance(String instanceid) {
        return toSimulatedInstance(simulatedCloud.getAgentInstance(instanceid));
    }

    public List<SimulatedTask> getSimulatedTasks(Set<String> taskIds) {
        return simulatedCloud.getAgentInstanceGroups().stream()
                .flatMap(g -> g.getAgents().stream())
                .flatMap(a -> a.getTaskExecutorHolders().stream())
                .filter(holder -> taskIds.isEmpty() || taskIds.contains(holder.getTaskId()))
                .map(this::toSimulatedTask)
                .collect(Collectors.toList());
    }

    public List<SimulatedTask> getSimulatedTasksOnInstance(String instanceId) {
        return simulatedCloud.getAgentInstance(instanceId).getTaskExecutorHolders().stream()
                .map(this::toSimulatedTask)
                .collect(Collectors.toList());
    }

    public void terminateAllTasks() {
        simulatedCloud.getAgentInstanceGroups().forEach(g -> g.getAgents().forEach(i ->
                i.getAllTasks().forEach(t ->
                        t.transitionTo(
                                Protos.TaskState.TASK_KILLED,
                                Protos.TaskStatus.Reason.REASON_EXECUTOR_TERMINATED,
                                "Terminated via cloud API (terminateAll)"
                        )
                )));
    }

    public boolean terminateTask(String taskId) {
        try {
            TaskExecutorHolder holder = simulatedCloud.getTaskExecutorHolder(taskId);
            holder.transitionTo(
                    Protos.TaskState.TASK_KILLED,
                    Protos.TaskStatus.Reason.REASON_EXECUTOR_TERMINATED,
                    "Terminated via cloud API (terminateTask)"
            );
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public void updateCapacity(String instanceGroupId, Capacity capacity) {
        simulatedCloud.getAgentInstanceGroup(instanceGroupId).updateCapacity(
                capacity.getMin(), capacity.getDesired(), capacity.getMax()
        );
    }

    public void terminateInstance(String instanceId, boolean shrink) {
        simulatedCloud.getAgentInstanceGroups().stream()
                .filter(g -> g.getAgents().stream().anyMatch(a -> a.getId().equals(instanceId)))
                .findFirst()
                .ifPresent(instanceGroup -> instanceGroup.terminate(instanceId, shrink));
    }

    private SimulatedInstanceGroup toSimulatedInstanceGroup(SimulatedTitusAgentCluster agentCluster) {
        return SimulatedInstanceGroup.newBuilder()
                .setId(agentCluster.getName())
                .setInstanceType(agentCluster.getInstanceType().getDescriptor().getId())
                .setCapacity(Capacity.newBuilder()
                        .setMin(agentCluster.getMinSize())
                        .setDesired(agentCluster.getAgents().size())
                        .setMax(agentCluster.getMaxSize())
                )
                .setComputeResources(SimulatedComputeResources.newBuilder()
                        .setCpu((int) agentCluster.getCpus())
                        .setMemoryMB(agentCluster.getMemory())
                        .setDiskMB(agentCluster.getDisk())
                        .setNetworkMB(agentCluster.getNetworkMbs())
                )
                .setIpPerEni(32)
                .addAllInstanceIds(agentCluster.getAgents().stream().map(SimulatedTitusAgent::getId).collect(Collectors.toList()))
                .build();
    }

    private SimulatedInstance toSimulatedInstance(SimulatedTitusAgent agent) {
        return SimulatedInstance.newBuilder()
                .setId(agent.getId())
                .setInstanceGroupId(agent.getClusterName())
                .setHostname(agent.getHostName())
                .setIpAddress(agent.getHostName())
                .setState(SimulatedInstance.SimulatedInstanceState.Running)
                .setAllComputeResources(SimulatedComputeResources.newBuilder()
                        .setCpu((int) agent.getTotalCPUs())
                        .setGpu((int) agent.getTotalGPUs())
                        .setMemoryMB(agent.getTotalMemory())
                        .setDiskMB(agent.getTotalDisk())
                        .setNetworkMB(agent.getTotalNetworkMbs())
                )
                .setAvailableComputeResources(SimulatedComputeResources.newBuilder()
                        .setCpu((int) agent.getAvailableCPUs())
                        .setGpu((int) agent.getAvailableGPUs())
                        .setMemoryMB(agent.getAvailableMemory())
                        .setDiskMB(agent.getAvailableDisk())
                        .setNetworkMB(agent.getAvailableNetworkMbs())
                )
                .build();
    }

    private SimulatedTask toSimulatedTask(TaskExecutorHolder holder) {
        Protos.TaskStatus taskStatus = holder.getTaskStatus();
        return SimulatedTask.newBuilder()
                .setTaskId(holder.getTaskId())
                .setInstanceId(holder.getAgent().getId())
                .setComputeResources(SimulatedComputeResources.newBuilder()
                        .setCpu((int) holder.getTaskCPUs())
                        .setMemoryMB((int) holder.getTaskMem())
                        .setDiskMB((int) holder.getTaskDisk())
                        .setNetworkMB((int) holder.getTaskNetworkMbs())
                )
                .setTaskState(taskStatus.getState().name())
                .setMessage(taskStatus.getMessage())
                .build();
    }
}
