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

package com.netflix.titus.testkit.stub.connector.cloud;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.tuple.Triple;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

public class TestableInstanceCloudConnector implements InstanceCloudConnector {

    public static final String DEFAULT_INSTANCE_TYPE = "defaultInstanceType";

    private int instanceGroupIndex;

    private final Map<String, Triple<InstanceGroup, Integer, Integer>> instanceGroupsById = new HashMap<>();
    private final Map<String, InstanceLaunchConfiguration> launchConfigurationsById = new HashMap<>();

    private final Map<String, Pair<Instance, Integer>> instancesById = new HashMap<>();
    private final Map<String, List<Consumer<String>>> interceptorsById = new HashMap<>();

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        return Observable.fromCallable(() -> instanceGroupsById.values().stream()
                .map(Triple::getFirst).collect(Collectors.toList()));
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        return Observable.fromCallable(() -> instanceGroupIds.stream()
                .filter(instanceGroupsById::containsKey)
                .map(id -> {
                    executeInterceptors(id);
                    return instanceGroupsById.get(id).getFirst();
                })
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
                .filter(instancesById::containsKey)
                .map(id -> instancesById.get(id).getLeft())
                .collect(Collectors.toList())
        );
    }

    @Override
    public Mono<Instance> getInstance(String instanceId) {
        return Mono.fromCallable(() -> {
            if (instancesById.containsKey(instanceId)) {
                return instancesById.get(instanceId).getLeft();
            }
            return null;
        });
    }

    @Override
    public Observable<List<Instance>> getInstancesByInstanceGroupId(String instanceGroupId) {
        return Observable.fromCallable(() -> instancesById.values().stream()
                .map(Pair::getLeft)
                .filter(instance -> instance.getInstanceGroupId().equals(instanceGroupId))
                .collect(Collectors.toList())
        );
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return null;
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        return null;
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroup, List<String> instanceIds, boolean shrink) {
        return null;
    }

    public void addInstanceGroup(InstanceGroup instanceGroup) {
        addInstanceGroup(instanceGroup, DEFAULT_INSTANCE_TYPE);
    }

    public void addInterceptor(String targetId, Consumer<String> interceptor) {
        interceptorsById.computeIfAbsent(targetId, id -> new ArrayList<>()).add(interceptor);
    }

    public void addInstanceGroup(InstanceGroup instanceGroup, String instanceType) {
        Triple<InstanceGroup, Integer, Integer> existing = instanceGroupsById.get(instanceGroup.getId());
        if (existing == null) {
            InstanceGroup effectiveInstanceGroup = instanceGroup.getInstanceIds().isEmpty()
                    ? instanceGroup
                    : instanceGroup.toBuilder().withInstanceIds(Collections.emptyList()).build();

            instanceGroupsById.put(instanceGroup.getId(), Triple.of(effectiveInstanceGroup, instanceGroupIndex++, 0));
            launchConfigurationsById.put(
                    instanceGroup.getLaunchConfigurationName(),
                    new InstanceLaunchConfiguration(instanceGroup.getLaunchConfigurationName(), instanceType)
            );
        } else {
            instanceGroupsById.put(
                    instanceGroup.getId(),
                    existing.mapFirst(previous -> instanceGroup.toBuilder().withInstanceIds(previous.getInstanceIds()).build())
            );
        }
    }

    public void removeInstanceGroup(String instanceGroupId) {
        Triple<InstanceGroup, Integer, Integer> instanceGroupEntry = instanceGroupsById.remove(instanceGroupId);
        if (instanceGroupEntry == null) {
            return;
        }
        InstanceGroup instanceGroup = instanceGroupEntry.getFirst();
        launchConfigurationsById.remove(instanceGroup.getLaunchConfigurationName());
        instanceGroup.getInstanceIds().forEach(instancesById::remove);
    }

    public void updateInstanceGroup(String instanceGroupid, InstanceGroup instanceGroup) {
        Triple<InstanceGroup, Integer, Integer> existing = instanceGroupsById.get(instanceGroupid);
        Preconditions.checkNotNull(existing);
        instanceGroupsById.put(
                instanceGroup.getId(),
                existing.mapFirst(previous -> instanceGroup.toBuilder().withInstanceIds(previous.getInstanceIds()).build())
        );
    }

    public void addInstance(Instance instance) {
        Triple<InstanceGroup, Integer, Integer> instanceGroupEntry = instanceGroupsById.get(instance.getInstanceGroupId());

        InstanceGroup instanceGroup = instanceGroupEntry.getFirst();
        String instanceId = instance.getId();
        int newInstanceIndex = instanceGroupEntry.getThird();

        boolean containsInstanceId = instanceGroup.getInstanceIds().stream().anyMatch(id -> id.equals(instanceId));
        if (containsInstanceId) {
            instancesById.put(instanceId, Pair.of(instance, instancesById.get(instanceId).getRight()));
        } else {
            InstanceGroup effectiveInstanceGroup = instanceGroup.toBuilder().withInstanceIds(CollectionsExt.copyAndAdd(instanceGroup.getInstanceIds(), instanceId)).build();
            instanceGroupsById.put(instance.getInstanceGroupId(), Triple.of(effectiveInstanceGroup, instanceGroupEntry.getSecond(), newInstanceIndex + 1));
            instancesById.put(instanceId, Pair.of(instance, newInstanceIndex));
        }
    }

    public void removeInstance(String instanceId) {
        Pair<Instance, Integer> instanceEntry = instancesById.remove(instanceId);
        if (instanceEntry == null) {
            return;
        }
        Instance instance = instanceEntry.getLeft();

        Triple<InstanceGroup, Integer, Integer> instanceGroupEntry = instanceGroupsById.get(instance.getInstanceGroupId());
        InstanceGroup instanceGroup = instanceGroupEntry.getFirst();
        InstanceGroup updatedInstanceGroup = instanceGroup.toBuilder()
                .withInstanceIds(
                        instanceGroup.getInstanceIds().stream().filter(id -> !id.equals(instanceId)).collect(Collectors.toList())
                ).build();
        instanceGroupsById.put(instance.getInstanceGroupId(), instanceGroupEntry.mapFirst(f -> updatedInstanceGroup));
    }

    public List<InstanceGroup> takeInstanceGroups() {
        return instanceGroupsById.values().stream().map(Triple::getFirst).collect(Collectors.toList());
    }

    public List<String> takeInstanceGroupIds() {
        return new ArrayList<>(instanceGroupsById.keySet());
    }

    public InstanceGroup takeInstanceGroup(int index) {
        return instanceGroupsById.values().stream()
                .filter(t -> t.getSecond() == index)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Instance group with index " + index + " not found"))
                .getFirst();
    }

    public List<Instance> takeInstances() {
        return instancesById.values().stream().map(Pair::getLeft).collect(Collectors.toList());
    }

    public List<String> takeInstanceIds() {
        return new ArrayList<>(instancesById.keySet());
    }

    public List<Instance> takeInstances(int instanceGroupIndex) {
        InstanceGroup instanceGroup = takeInstanceGroup(instanceGroupIndex);
        return instancesById.values().stream()
                .filter(instanceEntry -> instanceEntry.getLeft().getInstanceGroupId().equals(instanceGroup.getId()))
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }

    public Instance takeInstance(int instanceGroupIndex, int instanceIndex) {
        InstanceGroup instanceGroup = takeInstanceGroup(instanceGroupIndex);
        return instancesById.values().stream()
                .filter(instance -> instance.getLeft().getInstanceGroupId().equals(instanceGroup.getId()) && instance.getRight() == instanceIndex)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Instance with index " + instanceIndex + " not found in instance group " + instanceGroupIndex))
                .getLeft();
    }

    private void executeInterceptors(String id) {
        List<Consumer<String>> interceptors = interceptorsById.get(id);
        if (interceptors == null) {
            return;
        }
        interceptors.forEach(i -> i.accept(id));
    }
}
