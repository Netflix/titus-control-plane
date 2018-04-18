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

package com.netflix.titus.testkit.embedded.cell;

import java.util.Optional;

import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cell.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import rx.Observable;

public class EmbeddedCellTitusOperations implements EmbeddedTitusOperations {

    private final EmbeddedTitusMaster master;
    private final Optional<EmbeddedTitusGateway> gateway;
    private final SimulatedCloud simulatedCloud;

    public EmbeddedCellTitusOperations(EmbeddedTitusMaster master) {
        this(master, null);
    }

    public EmbeddedCellTitusOperations(EmbeddedTitusMaster master, EmbeddedTitusGateway gateway) {
        this.master = master;
        this.gateway = Optional.ofNullable(gateway);
        this.simulatedCloud = master.getSimulatedCloud();
    }

    @Override
    public SimulatedCloud getSimulatedCloud() {
        return simulatedCloud;
    }

    @Override
    public JobManagementServiceGrpc.JobManagementServiceStub getV3GrpcClient() {
        return gateway.map(EmbeddedTitusGateway::getV3GrpcClient).orElse(master.getV3GrpcClient());
    }

    @Override
    public JobManagementServiceGrpc.JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        return gateway.map(EmbeddedTitusGateway::getV3BlockingGrpcClient).orElse(master.getV3BlockingGrpcClient());
    }

    @Override
    public AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient() {
        return gateway.map(EmbeddedTitusGateway::getV3GrpcAgentClient).orElse(master.getV3GrpcAgentClient());
    }

    @Override
    public AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient() {
        return gateway.map(EmbeddedTitusGateway::getV3BlockingGrpcAgentClient).orElse(master.getV3BlockingGrpcAgentClient());
    }

    @Override
    public AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient() {
        return gateway.map(EmbeddedTitusGateway::getAutoScaleGrpcClient).orElse(master.getAutoScaleGrpcClient());
    }

    @Override
    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        return gateway.map(EmbeddedTitusGateway::getLoadBalancerGrpcClient).orElse(master.getLoadBalancerGrpcClient());
    }

    @Override
    public Observable<TaskExecutorHolder> observeLaunchedTasks() {
        return master.observeLaunchedTasks();
    }

    @Override
    public Observable<TaskExecutorHolder> awaitTaskExecutorHolderOf(String taskId) {
        return master.awaitTaskExecutorHolderOf(taskId);
    }
}
