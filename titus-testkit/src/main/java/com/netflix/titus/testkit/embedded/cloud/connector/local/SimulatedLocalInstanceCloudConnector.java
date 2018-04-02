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

package com.netflix.titus.testkit.embedded.cloud.connector.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import rx.Completable;
import rx.Observable;

/**
 * Connector used when {@link SimulatedCloud} runs in the same JVM.
 */
public class SimulatedLocalInstanceCloudConnector implements InstanceCloudConnector {

    private final SimulatedCloud cloud;

    public SimulatedLocalInstanceCloudConnector(SimulatedCloud cloud) {
        this.cloud = cloud;
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        return Observable.fromCallable(() -> cloud.getAgentInstanceGroups().stream().map(this::toInstanceGroup).collect(Collectors.toList()));
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        return Observable.fromCallable(() -> {
            Set<String> idSet = new HashSet<>(instanceGroupIds);
            List<InstanceGroup> instanceGroups = cloud.getAgentInstanceGroups().stream()
                    .filter(g -> idSet.contains(g.getName()))
                    .map(this::toInstanceGroup)
                    .collect(Collectors.toList());
            if (instanceGroups.size() != instanceGroupIds.size()) {
                Set<String> unknownIds = new HashSet<>(idSet);
                unknownIds.removeAll(instanceGroups.stream().map(InstanceGroup::getId).collect(Collectors.toList()));
                Preconditions.checkArgument(false, "Unknown instance groups requested: %s", unknownIds);
            }
            return instanceGroups;
        });
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        return Observable.fromCallable(() -> {
            Set<String> knownIds = cloud.getAgentInstanceGroups().stream().map(SimulatedTitusAgentCluster::getName).collect(Collectors.toSet());
            Set<String> unknownIds = new HashSet<>(launchConfigurationIds);
            unknownIds.removeAll(knownIds);
            Preconditions.checkArgument(unknownIds.isEmpty(), "Unknown instance groups requested: %s", unknownIds);

            List<InstanceLaunchConfiguration> launchConfigurations = launchConfigurationIds.stream()
                    .map(id -> new InstanceLaunchConfiguration(id, cloud.getAgentInstanceGroup(id).getInstanceType().getDescriptor().getId()))
                    .collect(Collectors.toList());
            return launchConfigurations;
        });
    }

    @Override
    public ResourceDimension getInstanceTypeResourceDimension(String instanceType) {
        return ResourceDimensions.fromAwsInstanceType(AwsInstanceType.withName(instanceType));
    }

    @Override
    public Observable<List<Instance>> getInstances(List<String> instanceIds) {
        return Observable.fromCallable(() -> instanceIds.stream().map(id -> toInstance(cloud.getAgentInstance(id))).collect(Collectors.toList()));
    }

    @Override
    public Observable<List<Instance>> getInstancesByInstanceGroupId(String instanceGroupId) {
        List<Instance> instancesByInstanceGroup = cloud.getAgentInstancesByInstanceGroup(instanceGroupId).stream()
                .map(this::toInstance)
                .collect(Collectors.toList());
        return Observable.just(instancesByInstanceGroup);
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return Completable.fromAction(() -> {
            SimulatedTitusAgentCluster agentInstanceGroup = cloud.getAgentInstanceGroup(instanceGroupId);
            agentInstanceGroup.updateCapacity(
                    min.orElse(agentInstanceGroup.getMinSize()),
                    desired.orElse(agentInstanceGroup.getAgents().size()),
                    agentInstanceGroup.getMaxSize()
            );
        });
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        return Completable.fromAction(() -> {
            SimulatedTitusAgentCluster agentInstanceGroup = cloud.getAgentInstanceGroup(instanceGroupId);
            agentInstanceGroup.updateCapacity(
                    agentInstanceGroup.getMinSize(),
                    agentInstanceGroup.getAgents().size() + scaleUpCount,
                    agentInstanceGroup.getMaxSize()
            );
        });
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink) {
        return Observable.fromCallable(() -> {
            SimulatedTitusAgentCluster agentInstanceGroup = cloud.getAgentInstanceGroup(instanceGroup);

            List<Either<Boolean, Throwable>> result = new ArrayList<>();
            for (String instanceId : instanceIds) {
                try {
                    agentInstanceGroup.terminate(instanceId, shrink);
                    result.add(Either.ofValue(true));
                } catch (Exception e) {
                    result.add(Either.ofError(e));
                }
            }
            return result;
        });
    }

    private InstanceGroup toInstanceGroup(SimulatedTitusAgentCluster agentCluster) {
        return InstanceGroup.newBuilder()
                .withId(agentCluster.getName())
                .withInstanceIds(agentCluster.getAgents().stream().map(SimulatedTitusAgent::getId).collect(Collectors.toList()))
                .withAttributes(Collections.emptyMap())
                .withMin(agentCluster.getMinSize())
                .withDesired(agentCluster.getAgents().size())
                .withMax(agentCluster.getMaxSize())
                .withLaunchConfigurationName(agentCluster.getName())
                .withIsLaunchSuspended(false)
                .withIsTerminateSuspended(false)
                .build();
    }

    private Instance toInstance(SimulatedTitusAgent agentInstance) {
        return Instance.newBuilder()
                .withId(agentInstance.getId())
                .withInstanceGroupId(agentInstance.getClusterName())
                .withInstanceState(Instance.InstanceState.Running)
                .withHostname(agentInstance.getHostName())
                .withIpAddress(agentInstance.getHostName())
                .withAttributes(Collections.emptyMap())
                .withLaunchTime(agentInstance.getLaunchTime())
                .build();
    }
}
