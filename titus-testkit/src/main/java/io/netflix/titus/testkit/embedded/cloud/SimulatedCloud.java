/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.embedded.cloud;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.mesos.MesosSchedulerCallbackHandler;
import io.netflix.titus.testkit.embedded.cloud.agent.OfferChangeEvent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedMesosSchedulerDriver;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import io.netflix.titus.testkit.embedded.cloud.resource.ComputeResources;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import rx.Observable;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;

public class SimulatedCloud {

    private final ComputeResources computeResources;
    private final ConcurrentMap<String, SimulatedTitusAgentCluster> agentInstanceGroups = new ConcurrentHashMap<>();

    private volatile int nextInstanceGroupId;

    private final SimulatedInstanceCloudConnector instanceCloudConnector;

    private SimulatedMesosSchedulerDriver mesosSchedulerDriver;

    public SimulatedCloud() {
        this.computeResources = new ComputeResources();
        this.instanceCloudConnector = new SimulatedInstanceCloudConnector(this);
    }

    public void shutdown() {
        agentInstanceGroups.values().forEach(SimulatedTitusAgentCluster::shutdown);
    }

    // FIXME Create proper contract between SimulatedCloud and MesosSchedulerDriver
    public void setMesosSchedulerDriver(SimulatedMesosSchedulerDriver mesosSchedulerDriver) {
        this.mesosSchedulerDriver = mesosSchedulerDriver;
        agentInstanceGroups.values().forEach(g -> g.getAgents().forEach(mesosSchedulerDriver::addAgent));
    }

    public SimulatedCloud addInstanceGroup(SimulatedTitusAgentCluster agentInstanceGroup) {
        agentInstanceGroups.put(agentInstanceGroup.getName(), agentInstanceGroup);
        if (mesosSchedulerDriver != null) {
            agentInstanceGroup.getAgents().forEach(a -> mesosSchedulerDriver.addAgent(a));
        }
        return this;
    }

    public SimulatedCloud createAgentInstanceGroups(SimulatedAgentGroupDescriptor... agentGroupDescriptors) {
        for (SimulatedAgentGroupDescriptor agentGroupDescriptor : agentGroupDescriptors) {
            Preconditions.checkArgument(
                    !agentInstanceGroups.containsKey(agentGroupDescriptor.getName()),
                    "Agent instance group with name %s already exists", agentGroupDescriptor.getName()
            );
            SimulatedTitusAgentCluster newAgentInstanceGroup = aTitusAgentCluster(agentGroupDescriptor.getName(), nextInstanceGroupId++)
                    .withComputeResources(computeResources)
                    .withCoolDownSec(60)
                    .withInstanceType(AwsInstanceType.withName(agentGroupDescriptor.getInstanceType()))
                    .withIpPerEni(agentGroupDescriptor.getIpPerEni())
                    .withSize(agentGroupDescriptor.getDesired())
                    .withMaxSize(agentGroupDescriptor.getMax())
                    .build();
            addInstanceGroup(newAgentInstanceGroup);
        }

        return this;
    }

    public List<SimulatedTitusAgentCluster> getAgentInstanceGroups() {
        return new ArrayList<>(agentInstanceGroups.values());
    }

    public SimulatedTitusAgentCluster getAgentInstanceGroup(String instanceGroupName) {
        SimulatedTitusAgentCluster simulatedTitusAgentCluster = agentInstanceGroups.get(instanceGroupName);
        Preconditions.checkNotNull(simulatedTitusAgentCluster, "Agent instance group %s not registered", instanceGroupName);
        return simulatedTitusAgentCluster;
    }

    public SimulatedTitusAgent getAgentInstance(String instanceName) {
        Optional<SimulatedTitusAgent> agentOptional = agentInstanceGroups.values().stream()
                .flatMap(g -> g.getAgents().stream())
                .filter(simulatedTitusAgent -> simulatedTitusAgent.getId().equals(instanceName))
                .findFirst();
        Preconditions.checkArgument(agentOptional.isPresent(), "Agent %s not found", instanceName);
        return agentOptional.get();
    }

    public ComputeResources getComputeResources() {
        return computeResources;
    }

    public TaskExecutorHolder getContainer(String containerId) {
        return agentInstanceGroups.values().stream()
                .flatMap(g -> g.getAgents().stream())
                .flatMap(a -> a.getAllTasks().stream())
                .filter(t -> t.getTaskId().equals(containerId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown container id " + containerId));
    }

    public void updateAgentGroupCapacity(String agentGroupId, int min, int desired, int max) {
        getAgentInstanceGroup(agentGroupId).updateCapacity(min, desired, max);
    }

    public void removeInstanceGroup(String instanceGroupName) {
        SimulatedTitusAgentCluster simulatedTitusAgentCluster = agentInstanceGroups.remove(instanceGroupName);
        if (simulatedTitusAgentCluster != null) {
            simulatedTitusAgentCluster.shutdown();
        }
    }

    public void removeInstance(String agentId) {
        agentInstanceGroups.values().stream()
                .filter(g -> g.getAgents().stream().anyMatch(a -> a.getId().equals(agentId)))
                .findFirst()
                .ifPresent(g -> {
                    g.terminate(agentId);
                });
    }

    public InstanceCloudConnector getInstanceCloudConnector() {
        return instanceCloudConnector;
    }

    public SchedulerDriver createMesosSchedulerDriver(Protos.FrameworkInfo framework, MesosSchedulerCallbackHandler scheduler) {
        return new SimulatedMesosSchedulerDriver(this, framework, scheduler);
    }

    public Observable<OfferChangeEvent> offerUpdates() {
        return null;
    }
}
