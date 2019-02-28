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

package com.netflix.titus.testkit.model.agent;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.model.ResourceDimensions;
import rx.Completable;
import rx.Observable;

class StubbedAgentManagementService implements AgentManagementService {

    private final StubbedAgentData stubbedAgentData;

    StubbedAgentManagementService(StubbedAgentData stubbedAgentData) {
        this.stubbedAgentData = stubbedAgentData;
    }

    @Override
    public ResourceDimension getResourceLimits(String instanceType) {
        return ResourceDimensions.fromAwsInstanceType(AwsInstanceType.withName(instanceType));
    }

    @Override
    public Optional<ResourceDimension> findResourceLimits(String instanceType) {
        try {
            return Optional.ofNullable(getResourceLimits(instanceType));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public Completable updateInstanceGroupTier(String instanceGroupId, Tier tier) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstanceGroup(instanceGroupId, previous -> previous.toBuilder().withTier(tier).build());
            return Completable.complete();
        });
    }

    @Override
    public Completable updateInstanceGroupLifecycle(String instanceGroupId, InstanceGroupLifecycleStatus instanceGroupLifecycleStatus) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstanceGroup(instanceGroupId, previous -> previous.toBuilder().withLifecycleStatus(instanceGroupLifecycleStatus).build());
            return Completable.complete();
        });
    }

    @Override
    public Completable updateInstanceGroupAttributes(String instanceGroupId, Map<String, String> attributes) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstanceGroup(instanceGroupId, previous -> {
                Map<String, String> updatedAttributes = CollectionsExt.merge(previous.getAttributes(), attributes);
                return previous.toBuilder().withAttributes(updatedAttributes).build();
            });
            return Completable.complete();
        });
    }

    @Override
    public Completable deleteInstanceGroupAttributes(String instanceGroupId, Set<String> keys) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstanceGroup(instanceGroupId, previous -> {
                Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(previous.getAttributes(), keys);
                return previous.toBuilder().withAttributes(updatedAttributes).build();
            });
            return Completable.complete();
        });
    }

    @Override
    public Completable updateAgentInstanceAttributes(String instanceId, Map<String, String> attributes) {
        stubbedAgentData.changeInstance(instanceId, previous -> {
            Map<String, String> updatedAttributes = CollectionsExt.merge(previous.getAttributes(), attributes);
            return previous.toBuilder().withAttributes(updatedAttributes).build();
        });
        return Completable.complete();
    }

    @Override
    public Completable deleteAgentInstanceAttributes(String instanceId, Set<String> keys) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstance(instanceId, previous -> {
                Map<String, String> updatedAttributes = CollectionsExt.copyAndRemove(previous.getAttributes(), keys);
                return previous.toBuilder().withAttributes(updatedAttributes).build();
            });
            return Completable.complete();
        });
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstanceGroup(instanceGroupId, previous -> {
                AgentInstanceGroup.Builder builder = previous.toBuilder();
                min.ifPresent(builder::withMin);
                desired.ifPresent(builder::withDesired);
                return builder.build();
            });
            return Completable.complete();
        });
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        return Completable.defer(() -> {
            stubbedAgentData.changeInstanceGroup(
                    instanceGroupId,
                    previous -> previous.toBuilder().withDesired(previous.getDesired() + scaleUpCount).build()
            );
            return Completable.complete();
        });
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateAgents(String instanceGroupId, List<String> instanceIds, boolean shrink) {
        return stubbedAgentData.terminateInstances(instanceGroupId, instanceIds, shrink);
    }

    @Override
    public void forceRefresh() {
    }

    @Override
    public Observable<AgentEvent> events(boolean includeSnapshot) {
        return stubbedAgentData.observeAgents(includeSnapshot);
    }

    @Override
    public List<AgentInstanceGroup> getInstanceGroups() {
        return stubbedAgentData.getInstanceGroups();
    }

    @Override
    public Optional<AgentInstanceGroup> findInstanceGroup(String instanceGroupId) {
        try {
            return Optional.ofNullable(stubbedAgentData.getInstanceGroup(instanceGroupId));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public List<AgentInstance> getAgentInstances(String instanceGroupId) {
        return stubbedAgentData.getInstances(instanceGroupId);
    }

    @Override
    public Optional<AgentInstance> findAgentInstance(String instanceId) {
        try {
            return Optional.ofNullable(stubbedAgentData.getInstance(instanceId));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Pair<AgentInstanceGroup, List<AgentInstance>>> findAgentInstances(Predicate<Pair<AgentInstanceGroup, AgentInstance>> filter) {
        return stubbedAgentData.getInstances().stream()
                .flatMap(p -> {
                    List<AgentInstance> matchingInstances = p.getRight().stream()
                            .filter(instance -> filter.test(Pair.of(p.getLeft(), instance)))
                            .collect(Collectors.toList());
                    return matchingInstances.isEmpty() ? Stream.empty() : Stream.of(Pair.of(p.getLeft(), matchingInstances));
                })
                .collect(Collectors.toList());
    }
}
