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

package com.netflix.titus.testkit.embedded.cloud.connector.remote;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.simulator.SimulatedAgentServiceGrpc;
import com.netflix.titus.simulator.SimulatedAgentServiceGrpc.SimulatedAgentServiceStub;
import com.netflix.titus.simulator.TitusCloudSimulator;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstance;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedInstanceGroup;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.netflix.titus.api.connector.cloud.Instance;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.InstanceGroup;
import com.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.grpc.GrpcUtil;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.model.ResourceDimensions;
import rx.Completable;
import rx.Observable;

@Singleton
public class SimulatedRemoteInstanceCloudConnector implements InstanceCloudConnector {

    private final ManagedChannel channel;
    private final SimulatedAgentServiceStub client;

    @Inject
    public SimulatedRemoteInstanceCloudConnector(CloudSimulatorResolver cloudSimulatorResolver) {
        Pair<String, Integer> cloudSimulatorAddress = cloudSimulatorResolver.resolveGrpcEndpoint();
        this.channel = ManagedChannelBuilder.forAddress(cloudSimulatorAddress.getLeft(), cloudSimulatorAddress.getRight())
                .usePlaintext(true)
                .build();
        this.client = SimulatedAgentServiceGrpc.newStub(channel);
    }

    @PreDestroy
    public void shutdown() {
        channel.shutdown();
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups() {
        return GrpcUtil.toObservable(client::getAllInstanceGroups).map(this::toInstanceGroup).toList();
    }

    @Override
    public Observable<List<InstanceGroup>> getInstanceGroups(List<String> instanceGroupIds) {
        TitusCloudSimulator.Ids ids = TitusCloudSimulator.Ids.newBuilder()
                .addAllIds(instanceGroupIds)
                .build();
        return GrpcUtil.toObservable(ids, client::getInstanceGroups).map(this::toInstanceGroup).toList();
    }

    @Override
    public Observable<List<InstanceLaunchConfiguration>> getInstanceLaunchConfiguration(List<String> launchConfigurationIds) {
        TitusCloudSimulator.Ids ids = TitusCloudSimulator.Ids.newBuilder()
                .addAllIds(launchConfigurationIds)
                .build();
        return GrpcUtil.toObservable(ids, client::getInstanceGroups).map(this::toLaunchConfiguration).toList();
    }

    @Override
    public ResourceDimension getInstanceTypeResourceDimension(String instanceType) {
        return ResourceDimensions.fromAwsInstanceType(AwsInstanceType.withName(instanceType));
    }

    @Override
    public Observable<List<Instance>> getInstances(List<String> instanceIds) {
        TitusCloudSimulator.Ids ids = TitusCloudSimulator.Ids.newBuilder()
                .addAllIds(instanceIds)
                .build();
        return GrpcUtil.toObservable(ids, client::getInstances).map(this::toInstance).toList();
    }

    @Override
    public Observable<List<Instance>> getInstancesByInstanceGroupId(String instanceGroupId) {
        return GrpcUtil.toObservable(
                TitusCloudSimulator.Id.newBuilder().setId(instanceGroupId).build(),
                client::getInstancesOfInstanceGroup
        ).map(this::toInstance).toList();
    }

    @Override
    public Completable updateCapacity(String instanceGroupId, Optional<Integer> min, Optional<Integer> desired) {
        return getInstanceGroups(Collections.singletonList(instanceGroupId))
                .flatMap(groups -> {
                    if (groups.isEmpty()) {
                        return Observable.error(new IllegalArgumentException("Unknown instance group " + instanceGroupId));
                    }
                    InstanceGroup instanceGroup = groups.get(0);
                    SimulatedInstanceGroup.Capacity capacity = SimulatedInstanceGroup.Capacity.newBuilder()
                            .setMin(min.orElse(instanceGroup.getMin()))
                            .setDesired(desired.orElse(instanceGroup.getDesired()))
                            .setMax(instanceGroup.getMax())
                            .build();
                    TitusCloudSimulator.CapacityUpdateRequest request = TitusCloudSimulator.CapacityUpdateRequest.newBuilder()
                            .setInstanceGroupId(instanceGroupId)
                            .setCapacity(capacity)
                            .build();
                    return GrpcUtil.toObservable(request, client::updateCapacity);
                }).toCompletable();
    }

    @Override
    public Completable scaleUp(String instanceGroupId, int scaleUpCount) {
        return getInstanceGroups(Collections.singletonList(instanceGroupId))
                .flatMap(groups -> {
                    if (groups.isEmpty()) {
                        return Observable.error(new IllegalArgumentException("Unknown instance group " + instanceGroupId));
                    }
                    InstanceGroup instanceGroup = groups.get(0);
                    SimulatedInstanceGroup.Capacity capacity = SimulatedInstanceGroup.Capacity.newBuilder()
                            .setMin(instanceGroup.getMin())
                            .setDesired(instanceGroup.getDesired() + scaleUpCount)
                            .setMax(instanceGroup.getMax())
                            .build();
                    TitusCloudSimulator.CapacityUpdateRequest request = TitusCloudSimulator.CapacityUpdateRequest.newBuilder()
                            .setInstanceGroupId(instanceGroupId)
                            .setCapacity(capacity)
                            .build();
                    return GrpcUtil.toObservable(request, client::updateCapacity);
                }).toCompletable();
    }

    @Override
    public Observable<List<Either<Boolean, Throwable>>> terminateInstances(String instanceGroupId, List<String> instanceIds, boolean shrink) {
        return getInstanceGroup(instanceGroupId).flatMap(instanceGroup -> {
            Set<String> actual = new HashSet<>(instanceGroup.getInstanceIds());
            Set<String> expected = new HashSet<>(instanceIds);
            Set<String> unknown = CollectionsExt.copyAndRemove(expected, actual);
            if (!unknown.isEmpty()) {
                return Observable.error(new IllegalArgumentException(String.format("Instance group %s does not contain instance ids %s", instanceGroupId, unknown)));
            }

            List<Observable<Pair<String, Optional<Throwable>>>> actions = instanceIds.stream()
                    .map(id -> ObservableExt.emitError(
                            GrpcUtil.toObservable(TitusCloudSimulator.Id.newBuilder().setId(id).build(),
                                    shrink ? client::terminateAndShrinkInstance : client::terminateInstance
                            )).map(r -> Pair.of(id, r)
                            ).toObservable()
                    ).collect(Collectors.toList());

            return Observable.merge(actions).toList().map(result -> {
                Either[] ordered = new Either[instanceIds.size()];
                for (int i = 0; i < instanceIds.size(); i++) {
                    String id = instanceIds.get(i);
                    Pair<String, Optional<Throwable>> terminateResult = result.stream()
                            .filter(p -> p.getLeft().equals(id))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("No terminate reply for instance id " + id));
                    ordered[i] = terminateResult.getRight().map(Either::ofError).orElseGet(() -> Either.ofValue(true));
                }
                return Arrays.asList(ordered);
            });
        });
    }

    private Observable<InstanceGroup> getInstanceGroup(String instanceGroupId) {
        return getInstanceGroups(Collections.singletonList(instanceGroupId))
                .flatMap(groups -> {
                    if (groups.isEmpty()) {
                        return Observable.error(new IllegalArgumentException("Unknown instance group " + instanceGroupId));
                    }
                    return Observable.just(groups.get(0));
                });
    }

    private InstanceGroup toInstanceGroup(SimulatedInstanceGroup simulated) {
        return InstanceGroup.newBuilder()
                .withId(simulated.getId())
                .withMin(simulated.getCapacity().getMin())
                .withDesired(simulated.getCapacity().getDesired())
                .withMax(simulated.getCapacity().getMax())
                .withInstanceIds(simulated.getInstanceIdsList())
                .withIsLaunchSuspended(false)
                .withIsTerminateSuspended(false)
                .withLaunchConfigurationName(simulated.getId())
                .withAttributes(Collections.emptyMap())
                .build();
    }

    private InstanceLaunchConfiguration toLaunchConfiguration(SimulatedInstanceGroup simulated) {
        return new InstanceLaunchConfiguration(simulated.getId(), simulated.getInstanceType());
    }

    private Instance toInstance(SimulatedInstance simulated) {
        Instance.InstanceState instanceState;
        switch (simulated.getState()) {
            case Running:
                instanceState = Instance.InstanceState.Running;
                break;
            case Terminated:
                instanceState = Instance.InstanceState.Terminated;
                break;
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException("Unexpected instance state: " + simulated.getState());
        }
        return Instance.newBuilder()
                .withId(simulated.getId())
                .withHostname(simulated.getHostname())
                .withIpAddress(simulated.getIpAddress())
                .withInstanceGroupId(simulated.getInstanceGroupId())
                .withInstanceState(instanceState)
                .withAttributes(simulated.getAttributesMap())
                .withLaunchTime(simulated.getLaunchTime())
                .build();
    }
}
