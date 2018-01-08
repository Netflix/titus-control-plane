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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.testkit.embedded.cloud.agent.AgentChangeEvent;
import io.netflix.titus.testkit.embedded.cloud.agent.OfferChangeEvent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.agent.player.ContainerPlayersManager;
import io.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import io.netflix.titus.testkit.embedded.cloud.resource.ComputeResources;
import org.apache.mesos.Protos;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;

public class SimulatedCloud {

    private final ComputeResources computeResources;

    private final ContainerPlayersManager containerPlayersManager = new ContainerPlayersManager(new DefaultRegistry(), Schedulers.computation());

    private final ConcurrentMap<String, SimulatedTitusAgentCluster> agentInstanceGroups = new ConcurrentHashMap<>();

    private final Subject<Observable<AgentChangeEvent>, Observable<AgentChangeEvent>> topologyEventsMergeSubject = new SerializedSubject<>(PublishSubject.create());
    private final Subject<AgentChangeEvent, AgentChangeEvent> instanceGroupAddedSubject = new SerializedSubject<>(PublishSubject.create());
    private final Observable<AgentChangeEvent> topologyUpdateObservable;

    private final Subject<Protos.TaskStatus, Protos.TaskStatus> lostTaskSubject = new SerializedSubject<>(PublishSubject.create());

    private volatile int nextInstanceGroupId;

    public SimulatedCloud() {
        this.computeResources = new ComputeResources();
        this.topologyUpdateObservable = Observable.merge(topologyEventsMergeSubject).mergeWith(instanceGroupAddedSubject).share();
    }

    public void shutdown() {
        agentInstanceGroups.values().forEach(SimulatedTitusAgentCluster::shutdown);
    }

    public SimulatedCloud addInstanceGroup(SimulatedTitusAgentCluster agentInstanceGroup) {
        agentInstanceGroups.put(agentInstanceGroup.getName(), agentInstanceGroup);
        instanceGroupAddedSubject.onNext(AgentChangeEvent.newInstanceGroup(agentInstanceGroup));
        topologyEventsMergeSubject.onNext(agentInstanceGroup.topologyUpdates());
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
                    .withContainerPlayersManager(containerPlayersManager)
                    .build();
            addInstanceGroup(newAgentInstanceGroup);
        }

        return this;
    }

    public ContainerPlayersManager getContainerPlayersManager() {
        return containerPlayersManager;
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

    public List<SimulatedTitusAgent> getAgentInstancesByInstanceGroup(String instanceGroupName) {
        Preconditions.checkArgument(agentInstanceGroups.containsKey(instanceGroupName), "Instance group %s not found", instanceGroupName);

        return agentInstanceGroups.get(instanceGroupName)
                .getAgents()
                .stream()
                .filter(simulatedTitusAgent -> simulatedTitusAgent.getClusterName().equals(instanceGroupName))
                .collect(Collectors.toList());
    }

    public ComputeResources getComputeResources() {
        return computeResources;
    }

    public TaskExecutorHolder getTaskExecutorHolder(String taskId) {
        return agentInstanceGroups.values().stream()
                .map(g -> g.findTaskById(taskId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown task id " + taskId));
    }

    public Optional<SimulatedTitusAgent> getAgentOwningOffer(String offerId) {
        return agentInstanceGroups.values().stream()
                .map(g -> g.findAgentWithOffer(offerId))
                .filter(Optional::isPresent).map(Optional::get)
                .findFirst();
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
        agentInstanceGroups.values().forEach(g -> {
            g.getAgents().stream().filter(a -> a.getId().equals(agentId)).findFirst().ifPresent(a -> {
                g.terminate(agentId, false);
            });
        });
    }

    public void declineOffer(String offerId) {
        getAgentOwningOffer(offerId).ifPresent(agent -> agent.declineOffer(offerId));
    }

    public List<TaskExecutorHolder> launchTasks(String offerId, Collection<Protos.TaskInfo> tasks) {
        if (tasks.isEmpty()) {
            declineOffer(offerId);
            return Collections.emptyList();
        }
        Optional<SimulatedTitusAgent> agent = getAgentOwningOffer(offerId);
        if (agent.isPresent()) {
            return agent.get().launchTasks(offerId, tasks);
        }

        tasks.forEach(task -> emitTaskLostEvent(task.getTaskId().getValue(), "Task launched with invalid offer: " + offerId));

        return Collections.emptyList();
    }

    public void killTask(String taskId) {
        try {
            getTaskExecutorHolder(taskId).transitionTo(Protos.TaskState.TASK_KILLED);
        } catch (IllegalArgumentException e) {
            emitTaskLostEvent(taskId, "Simulated cloud task kill failed with an error: " + e.getMessage());
        }
    }

    public void reconcileTasks(Set<String> taskIds) {
        Set<String> found = agentInstanceGroups.values().stream()
                .flatMap(g -> g.reconcileOwnedTasksIgnoreOther(taskIds).stream())
                .collect(Collectors.toSet());
        CollectionsExt.copyAndRemove(taskIds, found).forEach(lostTaskId -> emitTaskLostEvent(lostTaskId, "Task not found"));
    }

    public void reconcileKnownTasks() {
        agentInstanceGroups.values().forEach(SimulatedTitusAgentCluster::reconcileKnownTasks);
    }

    public Observable<AgentChangeEvent> topologyUpdates() {
        return topologyUpdateObservable;
    }

    public Observable<OfferChangeEvent> offers() {
        return topologyUpdates()
                .filter(e -> e.getEventType() == AgentChangeEvent.EventType.InstanceGroupCreated)
                .compose(ObservableExt.head(this::toNewInstanceGroupEvents))
                .distinct(event -> event.getInstanceGroup().getName())
                .flatMap(event -> event.getInstanceGroup().observeOffers());
    }

    public Observable<Protos.TaskStatus> taskStatusUpdates() {
        return Observable.merge(
                topologyUpdates()
                        .compose(ObservableExt.head(this::toNewInstanceGroupEvents))
                        .flatMap(event -> event.getInstanceGroup().taskStatusUpdates()),
                lostTaskSubject
        );
    }

    public Observable<TaskExecutorHolder> taskLaunches() {
        return topologyUpdates()
                .compose(ObservableExt.head(this::toNewInstanceGroupEvents))
                .flatMap(event -> event.getInstanceGroup().taskLaunches());
    }

    private void emitTaskLostEvent(String lostTaskId, String reason) {
        lostTaskSubject.onNext(Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(lostTaskId).build())
                .setState(Protos.TaskState.TASK_LOST)
                .setMessage(reason)
                .build()
        );
    }

    private List<AgentChangeEvent> toNewInstanceGroupEvents() {
        return agentInstanceGroups.values().stream()
                .map(AgentChangeEvent::newInstanceGroup)
                .collect(Collectors.toList());
    }
}
