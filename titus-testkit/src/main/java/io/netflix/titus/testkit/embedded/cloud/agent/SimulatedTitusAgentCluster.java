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

package io.netflix.titus.testkit.embedded.cloud.agent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.testkit.embedded.cloud.resource.ComputeResources;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import rx.schedulers.Schedulers;

import static java.util.Arrays.asList;

/**
 */
public class SimulatedTitusAgentCluster {

    private static final int DEFAULT_MAX_SIZE = 100;

    private final String name;
    private final ComputeResources computeResources;
    private final AwsInstanceType instanceType;
    private final double cpus;
    private final double gpus;
    private final int memory;
    private final int disk;
    private final int networkMbs;
    private final int ipPerEni;
    private final AutoScaleRule autoScaleRule;

    private int minSize = 0;
    private int maxSize = DEFAULT_MAX_SIZE;

    private Protos.Offer.Builder offerTemplate;

    private final List<SimulatedTitusAgent> agents = new ArrayList<>();

    private SimulatedTitusAgentCluster(String name,
                                       ComputeResources computeResources,
                                       AwsInstanceType instanceType,
                                       double cpus,
                                       double gpus,
                                       int memory,
                                       int disk,
                                       int networkMbs,
                                       int ipPerEni,
                                       AutoScaleRule autoScaleRule) {
        this.name = name;
        this.computeResources = computeResources;
        this.instanceType = instanceType;
        this.cpus = cpus;
        this.gpus = gpus;
        this.memory = memory;
        this.disk = disk;
        this.networkMbs = networkMbs;
        this.ipPerEni = ipPerEni;
        this.autoScaleRule = autoScaleRule;

        this.offerTemplate = Protos.Offer.newBuilder()
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("EmbeddedTitusMaster"))
                .addAllAttributes(asList(
                        Attribute.newBuilder().setName("CLUSTER_NAME").setType(Type.SCALAR).setText(Text.newBuilder().setValue(name)).build(),
                        Attribute.newBuilder().setName("region").setType(Type.SCALAR).setText(Text.newBuilder().setValue("us-east-1")).build(),
                        Attribute.newBuilder().setName("stack").setType(Type.SCALAR).setText(Text.newBuilder().setValue("local")).build(),
                        Attribute.newBuilder().setName("itype").setType(Type.TEXT).setText(Text.newBuilder().setValue(instanceType.name())).build()
                ));
    }

    public void shutdown() {
        agents.forEach(SimulatedTitusAgent::shutdown);
    }

    public String getName() {
        return name;
    }

    public AwsInstanceType getInstanceType() {
        return instanceType;
    }

    public double getCpus() {
        return cpus;
    }

    public double getGpus() {
        return gpus;
    }

    public int getMemory() {
        return memory;
    }

    public int getDisk() {
        return disk;
    }

    public int getNetworkMbs() {
        return networkMbs;
    }

    public int getMinSize() {
        return minSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public AutoScaleRule getAutoScaleRule() {
        return autoScaleRule;
    }

    public List<SimulatedTitusAgent> getAgents() {
        return agents;
    }

    public void updateCapacity(int min, int desired, int max) {
        Preconditions.checkArgument(min >= 0, "min (%s) < 0", min);
        Preconditions.checkArgument(min <= desired, "min (%s) > desired (%s)", min, desired);
        Preconditions.checkArgument(desired <= max, "desired (%s) > max (%s)", desired, max);

        this.minSize = min;
        this.maxSize = max;

        int current = this.agents.size();
        if (current != desired) {
            if (current > desired) {
                scaleDown(current - desired);
            } else {
                scaleUp(desired - current);
            }
        }
    }

    public void scaleUp(int count) {
        for (int i = 0; i < count; i++) {
            createAgent();
        }
    }

    public void scaleDown(int count) {
        int last = agents.size() - count;
        for (int i = agents.size() - 1; i >= last; i--) {
            agents.remove(i).shutdown();
        }
    }

    public void terminate(String agentId) {
        Iterator<SimulatedTitusAgent> it = agents.iterator();
        while (it.hasNext()) {
            SimulatedTitusAgent agent = it.next();
            if (agent.getId().equals(agentId)) {
                it.remove();
                agent.shutdown();
            }
        }
    }

    public Optional<TaskExecutorHolder> findTaskById(String taskId) {
        for (SimulatedTitusAgent agent : agents) {
            Optional<TaskExecutorHolder> holder = agent.findTaskById(taskId);
            if (holder.isPresent()) {
                return holder;
            }
        }
        return Optional.empty();
    }

    private SimulatedTitusAgent createAgent() {
        String hostName = ComputeResources.asHostname(computeResources.allocateIpAddress());
        Protos.SlaveID slaveId = Protos.SlaveID.newBuilder().setValue(hostName).build();

        Protos.Offer.Builder agentOfferTemplate = offerTemplate.clone()
                .setHostname(hostName)
                .setUrl(Protos.URL.newBuilder()
                        .setScheme("http")
                        .setAddress(Protos.Address.newBuilder().setHostname(hostName).setPort(5051))
                        .setPath("slave")
                );

        SimulatedTitusAgent agent = new SimulatedTitusAgent(name, computeResources, hostName, slaveId, agentOfferTemplate, instanceType,
                cpus, gpus, memory, disk, networkMbs, ipPerEni, Schedulers.computation());
        agents.add(agent);
        return agent;
    }

    public static Builder aTitusAgentCluster(String name, int idx) {
        return new Builder(name, idx);
    }

    public static class Builder {

        private final String name;
        private final int idx;
        private AwsInstanceType instanceType;
        private ComputeResources computeResources;
        private double cpus = 8;
        private int memory = 8192;
        private int disk = 200000;
        private int networkMbs = 1000;
        private int size = 2;
        private int gpus = 0;

        private int minIdleHostsToKeep = 1;
        private int maxIdleHostsToKeep = 10;
        private long coolDownSec = 30;
        private int ipPerEni = 29;

        private Builder(String name, int idx) {
            this.name = name;
            this.idx = idx;
        }

        public Builder withComputeResources(ComputeResources computeResources) {
            this.computeResources = computeResources;
            return this;
        }

        public Builder withInstanceType(AwsInstanceType instanceType) {
            AwsInstanceDescriptor descriptor = instanceType.getDescriptor();
            cpus = descriptor.getvCPUs();
            gpus = descriptor.getvGPUs();
            memory = descriptor.getMemoryGB() * 1024;
            disk = descriptor.getStorageGB() * 1024;
            networkMbs = descriptor.getNetworkMbs();
            this.instanceType = instanceType;
            return this;
        }

        public Builder withSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withMinIdleHostsToKeep(int minIdleHostsToKeep) {
            this.minIdleHostsToKeep = minIdleHostsToKeep;
            return this;
        }

        public Builder withMaxIdleHostsToKeep(int maxIdleHostsToKeep) {
            this.maxIdleHostsToKeep = maxIdleHostsToKeep;
            return this;
        }

        public Builder withCoolDownSec(long coolDownSec) {
            this.coolDownSec = coolDownSec;
            return this;
        }

        public Builder withIpPerEni(int ipPerEni) {
            this.ipPerEni = ipPerEni;
            return this;
        }

        public SimulatedTitusAgentCluster build() {
            AutoScaleRule autoScaleRule = AutoScaleRule.newBuilder()
                    .withMinIdleToKeep(minIdleHostsToKeep)
                    .withMaxIdleToKeep(maxIdleHostsToKeep)
                    .withCoolDownSec((int) coolDownSec)
                    .withShortfallAdjustingFactor(1)
                    .build();
            SimulatedTitusAgentCluster agentCluster = new SimulatedTitusAgentCluster(
                    name, computeResources, instanceType, cpus, gpus, memory, disk, networkMbs, ipPerEni,
                    autoScaleRule
            );
            agentCluster.scaleUp(size);
            return agentCluster;
        }
    }
}
