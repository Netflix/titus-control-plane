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

package com.netflix.titus.testkit.embedded.federation;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.*;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import rx.Observable;

class EmbeddedFederationTitusOperations implements EmbeddedTitusOperations {

    private final EmbeddedTitusFederation federation;
    private final SimulatedCloud cloudSimulator;

    EmbeddedFederationTitusOperations(EmbeddedTitusFederation federation) {
        this.federation = federation;
        // We assume, a single cloud simulator instance is shared between all cells.
        this.cloudSimulator = this.federation.getCells().get(0).getTitusOperations().getSimulatedCloud();
    }

    @Override
    public SimulatedCloud getSimulatedCloud() {
        return cloudSimulator;
    }

    @Override
    public HealthGrpc.HealthStub getHealthClient() {
        return federation.getHealthGrpcClient();
    }

    @Override
    public SchedulerServiceGrpc.SchedulerServiceBlockingStub getV3BlockingSchedulerClient() {
        return federation.getV3BlockingSchedulerClient();
    }

    @Override
    public JobManagementServiceGrpc.JobManagementServiceStub getV3GrpcClient() {
        return federation.getV3GrpcClient();
    }

    @Override
    public JobManagementServiceGrpc.JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        return federation.getV3BlockingGrpcClient();
    }

    /**
     * FIXME Agent management is at cell level. This API must be changed for multi-cell support.
     */
    @Override
    public AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient() {
        return federation.getCells().get(0).getTitusOperations().getV3GrpcAgentClient();
    }

    /**
     * FIXME Agent management is at cell level. This API must be changed for multi-cell support.
     */
    @Override
    public AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient() {
        return federation.getCells().get(0).getTitusOperations().getV3BlockingGrpcAgentClient();
    }

    @Override
    public AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient() {
        return federation.getAutoScaleGrpcClient();
    }

    @Override
    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        return federation.getLoadBalancerGrpcClient();
    }

    @Override
    public JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub getJobActivityHistoryGrpcClient() {
        return federation.getJobActivityGrpcClient();
    }

    @Override
    public EvictionServiceGrpc.EvictionServiceBlockingStub getBlockingGrpcEvictionClient() {
        return federation.getBlockingGrpcEvictionClient();
    }

    @Override
    public MachineServiceGrpc.MachineServiceBlockingStub getBlockingGrpcMachineClient() {
        return federation.getBlockingGrpcMachineClient();
    }

    @Override
    public Observable<TaskExecutorHolder> observeLaunchedTasks() {
        List<Observable<TaskExecutorHolder>> observableList = federation.getCells()
                .stream()
                .map(c -> c.getTitusOperations().observeLaunchedTasks())
                .collect(Collectors.toList());
        return Observable.merge(observableList);
    }

    @Override
    public Observable<TaskExecutorHolder> awaitTaskExecutorHolderOf(String taskId) {
        List<Observable<TaskExecutorHolder>> observableList = federation.getCells()
                .stream()
                .map(c -> c.getTitusOperations().awaitTaskExecutorHolderOf(taskId))
                .collect(Collectors.toList());
        return Observable.merge(observableList).take(1);
    }
}
