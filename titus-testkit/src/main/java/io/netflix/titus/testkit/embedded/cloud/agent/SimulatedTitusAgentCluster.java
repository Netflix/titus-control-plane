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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.netflix.titus.api.agent.model.AutoScaleRule;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.testkit.embedded.cloud.agent.player.ContainerPlayersManager;
import io.netflix.titus.testkit.embedded.cloud.resource.ComputeResources;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import static java.util.Arrays.asList;

/**
 */
public class SimulatedTitusAgentCluster {

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
    private int maxSize;
    private final ContainerPlayersManager containerPlayersManager;

    private Protos.Offer.Builder offerTemplate;

    private final ConcurrentMap<String, SimulatedTitusAgent> agents = new ConcurrentHashMap<>();

    private final Subject<AgentChangeEvent, AgentChangeEvent> topologUpdateSubject = new SerializedSubject<>(PublishSubject.create());

    private SimulatedTitusAgentCluster(String name,
                                       ComputeResources computeResources,
                                       AwsInstanceType instanceType,
                                       double cpus,
                                       double gpus,
                                       int memory,
                                       int disk,
                                       int networkMbs,
                                       int ipPerEni,
                                       AutoScaleRule autoScaleRule,
                                       ContainerPlayersManager containerPlayersManager) {
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
        this.maxSize = autoScaleRule.getMax();
        this.containerPlayersManager = containerPlayersManager;

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
        agents.values().forEach(SimulatedTitusAgent::shutdown);
        topologUpdateSubject.onNext(AgentChangeEvent.terminatedInstanceGroup(this));
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
        return new ArrayList<>(agents.values());
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
        Iterator<SimulatedTitusAgent> it = agents.values().iterator();
        for (int i = 0; i < count && it.hasNext(); i++) {
            SimulatedTitusAgent toRemove = it.next();
            it.remove();
            toRemove.shutdown();
            topologUpdateSubject.onNext(AgentChangeEvent.terminatedInstance(this, toRemove));
        }
    }

    public void terminate(String agentId, boolean shrink) {
        SimulatedTitusAgent toRemove = agents.remove(agentId);
        if (toRemove != null) {
            toRemove.shutdown();
            if (!shrink) {
                scaleUp(1);
            }
        }
    }

    public Optional<TaskExecutorHolder> findTaskById(String taskId) {
        for (SimulatedTitusAgent agent : agents.values()) {
            Optional<TaskExecutorHolder> holder = agent.findTaskById(taskId);
            if (holder.isPresent()) {
                return holder;
            }
        }
        return Optional.empty();
    }

    public Set<String> reconcileOwnedTasksIgnoreOther(Set<String> taskIds) {
        return agents.values().stream().flatMap(a -> a.reconcileOwnedTasksIgnoreOther(taskIds).stream()).collect(Collectors.toSet());
    }

    public Set<String> reconcileKnownTasks() {
        return agents.values().stream().flatMap(a -> a.reconcileKnownTasks().stream()).collect(Collectors.toSet());
    }

    public Optional<SimulatedTitusAgent> findAgentWithOffer(String offerId) {
        return agents.values().stream().filter(a -> a.isOfferOwner(offerId)).findFirst();
    }

    public Observable<AgentChangeEvent> topologyUpdates() {
        return topologUpdateSubject;
    }

    public Observable<TaskExecutorHolder> taskLaunches() {
        return observe(SimulatedTitusAgent::observeTaskLaunches);
    }

    public Observable<Protos.TaskStatus> taskStatusUpdates() {
        return observe(SimulatedTitusAgent::taskStatusUpdates);
    }

    public Observable<OfferChangeEvent> observeOffers() {
        return observe(SimulatedTitusAgent::observeOffers);
    }

    private <T> Observable<T> observe(Function<SimulatedTitusAgent, Observable<T>> mapper) {
        return topologyUpdates()
                .filter(event -> event.getEventType() == AgentChangeEvent.EventType.InstanceCreated)
                .compose(ObservableExt.head(this::toNewInstanceEvents))
                .distinct(event -> event.getInstance().get().getId())
                .flatMap(event -> mapper.apply(event.getInstance().get()));
    }

    private List<AgentChangeEvent> toNewInstanceEvents() {
        return agents.values().stream()
                .map(a -> AgentChangeEvent.newInstance(this, a))
                .collect(Collectors.toList());
    }

    private SimulatedTitusAgent createAgent() {
        String hostName = ComputeResources.asHostname(computeResources.allocateIpAddress(), name);
        Protos.SlaveID slaveId = Protos.SlaveID.newBuilder().setValue(hostName).build();

        Protos.Offer.Builder agentOfferTemplate = offerTemplate.clone()
                .setHostname(hostName)
                .setUrl(Protos.URL.newBuilder()
                        .setScheme("http")
                        .setAddress(Protos.Address.newBuilder().setHostname(hostName).setPort(5051))
                        .setPath("slave")
                );

        SimulatedTitusAgent agent = new SimulatedTitusAgent(name, computeResources, hostName, slaveId, agentOfferTemplate, instanceType,
                cpus, gpus, memory, disk, networkMbs, ipPerEni, containerPlayersManager, Schedulers.computation());
        agents.put(agent.getId(), agent);
        topologUpdateSubject.onNext(AgentChangeEvent.newInstance(this, agent));
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
        private int maxSize = 2;
        private int gpus = 0;

        private int minIdleHostsToKeep = 1;
        private int maxIdleHostsToKeep = 10;
        private long coolDownSec = 30;
        private int ipPerEni = 29;
        private ContainerPlayersManager containerPlayersManager;

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

        public Builder withMaxSize(int maxSize) {
            this.maxSize = maxSize;
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

        public Builder withContainerPlayersManager(ContainerPlayersManager containerPlayersManager) {
            this.containerPlayersManager = containerPlayersManager;
            return this;
        }

        public SimulatedTitusAgentCluster build() {
            Preconditions.checkNotNull(containerPlayersManager, "ContainerPlayersManager not defined");

            AutoScaleRule autoScaleRule = AutoScaleRule.newBuilder()
                    .withMinIdleToKeep(minIdleHostsToKeep)
                    .withMaxIdleToKeep(maxIdleHostsToKeep)
                    .withMax(maxSize)
                    .withCoolDownSec((int) coolDownSec)
                    .withShortfallAdjustingFactor(1)
                    .build();

            SimulatedTitusAgentCluster agentCluster = new SimulatedTitusAgentCluster(
                    name, computeResources, instanceType, cpus, gpus, memory, disk, networkMbs, ipPerEni,
                    autoScaleRule, containerPlayersManager
            );
            agentCluster.scaleUp(size);
            return agentCluster;
        }
    }
}
