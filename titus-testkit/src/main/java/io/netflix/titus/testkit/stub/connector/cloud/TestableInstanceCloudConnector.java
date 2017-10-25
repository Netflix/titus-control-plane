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

package io.netflix.titus.testkit.stub.connector.cloud;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.tuple.Either;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.common.util.tuple.Triple;
import rx.Completable;
import rx.Observable;

public class TestableInstanceCloudConnector implements InstanceCloudConnector {

    public static final String DEFAULT_INSTANCE_TYPE = "defaultInstanceType";

    private int serverGroupIdx;

    private final Map<String, Triple<InstanceGroup, Integer, Integer>> serverGroupsById = new HashMap<>();
    private final Map<String, InstanceLaunchConfiguration> launchConfigurationsById = new HashMap<>();

    private final Map<String, Pair<Instance, Integer>> serversById = new HashMap<>();

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        return Observable.fromCallable(() -> serverGroupsById.values().stream().map(Triple::getFirst).collect(Collectors.toList()));
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        return Observable.fromCallable(() -> instanceGroupIds.stream()
                .filter(serverGroupsById::containsKey)
                .map(id -> serverGroupsById.get(id).getFirst())
                .collect(Collectors.toList()));
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        return Observable.fromCallable(() -> launchConfigurationIds.stream()
                .filter(launchConfigurationsById::containsKey)
                .map(launchConfigurationsById::get)
                .collect(Collectors.toList())
        );
    }

    @Override
    public ResourceDimension getInstanceTypeResourceDimension(String instanceType) {
        return ResourceDimension.empty();
    }

    @Override
    public Observable<List<Instance>> getInstances(List<String> instanceIds) {
        return Observable.fromCallable(() -> instanceIds.stream()
                .filter(serversById::containsKey)
                .map(id -> serversById.get(id).getLeft())
                .collect(Collectors.toList())
        );
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return null;
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink) {
        return null;
    }

    public void addVmServerGroup(InstanceGroup instanceGroup) {
        addVmServerGroup(instanceGroup, DEFAULT_INSTANCE_TYPE);
    }

    public void addVmServerGroup(InstanceGroup instanceGroup, String instanceType) {
        Triple<InstanceGroup, Integer, Integer> existing = serverGroupsById.get(instanceGroup.getId());
        if (existing == null) {
            InstanceGroup effectiveServerGroup = instanceGroup.getInstanceIds().isEmpty()
                    ? instanceGroup
                    : instanceGroup.toBuilder().withInstanceIds(Collections.emptyList()).build();

            serverGroupsById.put(instanceGroup.getId(), Triple.of(effectiveServerGroup, serverGroupIdx++, 0));
            launchConfigurationsById.put(
                    instanceGroup.getLaunchConfigurationName(),
                    new InstanceLaunchConfiguration(instanceGroup.getLaunchConfigurationName(), instanceType)
            );
        } else {
            serverGroupsById.put(
                    instanceGroup.getId(),
                    existing.mapFirst(previous -> instanceGroup.toBuilder().withInstanceIds(previous.getInstanceIds()).build())
            );
        }
    }

    public void removeVmServerGroup(String serverGroupId) {
        Triple<InstanceGroup, Integer, Integer> serverGroupEntry = serverGroupsById.remove(serverGroupId);
        if (serverGroupEntry == null) {
            return;
        }
        InstanceGroup serverGroup = serverGroupEntry.getFirst();
        launchConfigurationsById.remove(serverGroup.getLaunchConfigurationName());
        serverGroup.getInstanceIds().forEach(serversById::remove);
    }

    public void addVmServer(Instance instance) {
        Triple<InstanceGroup, Integer, Integer> serverGroupEntry = serverGroupsById.get(instance.getInstanceGroupId());

        InstanceGroup serverGroup = serverGroupEntry.getFirst();
        String vmServerId = instance.getId();
        int newVmIndex = serverGroupEntry.getThird();

        boolean containsVmId = serverGroup.getInstanceIds().stream().anyMatch(id -> id.equals(vmServerId));
        if (containsVmId) {
            serversById.put(vmServerId, Pair.of(instance, serversById.get(vmServerId).getRight()));
        } else {
            InstanceGroup effectiveServerGroup = serverGroup.toBuilder().withInstanceIds(CollectionsExt.copyAndAdd(serverGroup.getInstanceIds(), vmServerId)).build();
            serverGroupsById.put(instance.getInstanceGroupId(), Triple.of(effectiveServerGroup, serverGroupEntry.getSecond(), newVmIndex + 1));
            serversById.put(vmServerId, Pair.of(instance, newVmIndex));
        }
    }

    public void removeVmServer(String vmServerId) {
        Pair<Instance, Integer> serverEntry = serversById.remove(vmServerId);
        if (serverEntry == null) {
            return;
        }
        Instance instance = serverEntry.getLeft();

        Triple<InstanceGroup, Integer, Integer> serverGroupEntry = serverGroupsById.get(instance.getInstanceGroupId());
        InstanceGroup serverGroup = serverGroupEntry.getFirst();
        InstanceGroup updatedServerGroup = serverGroup.toBuilder()
                .withInstanceIds(
                        serverGroup.getInstanceIds().stream().filter(vmId -> !vmId.equals(vmServerId)).collect(Collectors.toList())
                ).build();
        serverGroupsById.put(instance.getInstanceGroupId(), serverGroupEntry.mapFirst(f -> updatedServerGroup));
    }

    public List<InstanceGroup> takeServerGroups() {
        return serverGroupsById.values().stream().map(Triple::getFirst).collect(Collectors.toList());
    }

    public List<String> takeServerGroupIds() {
        return new ArrayList<>(serverGroupsById.keySet());
    }

    public InstanceGroup takeServerGroup(int idx) {
        return serverGroupsById.values().stream()
                .filter(t -> t.getSecond() == idx)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Server group with index " + idx + " not found"))
                .getFirst();
    }

    public List<Instance> takeServers() {
        return serversById.values().stream().map(Pair::getLeft).collect(Collectors.toList());
    }

    public List<String> takeServerIds() {
        return new ArrayList<>(serversById.keySet());
    }

    public List<Instance> takeServers(int serverGroupIdx) {
        InstanceGroup serverGroup = takeServerGroup(serverGroupIdx);
        return serversById.values().stream()
                .filter(serverEntry -> serverEntry.getLeft().getInstanceGroupId().equals(serverGroup.getId()))
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }

    public Instance takeServer(int serverGroupIdx, int serverIdx) {
        InstanceGroup serverGroup = takeServerGroup(serverGroupIdx);
        return serversById.values().stream()
                .filter(server -> server.getLeft().getInstanceGroupId().equals(serverGroup.getId()) && server.getRight() == serverIdx)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Server with index " + serverIdx + " not found in server group " + serverGroupIdx))
                .getLeft();
    }
}
