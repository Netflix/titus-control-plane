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

package com.netflix.titus.testkit.embedded;

import java.util.Optional;

import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.embedded.federation.EmbeddedTitusFederation;
import com.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import rx.Observable;

public class EmbeddedTitusOperations {

    private final EmbeddedTitusMaster master;
    private final Optional<EmbeddedTitusGateway> gateway;
    private final SimulatedCloud simulatedCloud;
    private final Optional<EmbeddedTitusFederation> federation;

    public EmbeddedTitusOperations(EmbeddedTitusMaster master) {
        this(master, null, Optional.empty());
    }

    public EmbeddedTitusOperations(EmbeddedTitusMaster master, EmbeddedTitusGateway gateway, Optional<EmbeddedTitusFederation> federation) {
        this.master = master;
        this.gateway = Optional.ofNullable(gateway);
        this.simulatedCloud = master.getSimulatedCloud();
        this.federation = federation;
    }

    public SimulatedCloud getSimulatedCloud() {
        return simulatedCloud;
    }

    public JobManagementServiceGrpc.JobManagementServiceStub getV3GrpcClient() {
        return federation
                .map(EmbeddedTitusFederation::getV3GrpcClient)
                .orElse(gateway.map(EmbeddedTitusGateway::getV3GrpcClient).orElse(master.getV3GrpcClient()));
    }

    public JobManagementServiceGrpc.JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        return federation
                .map(EmbeddedTitusFederation::getV3BlockingGrpcClient)
                .orElse(gateway.map(EmbeddedTitusGateway::getV3BlockingGrpcClient).orElse(master.getV3BlockingGrpcClient()));
    }

    public AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient() {
        return gateway.map(EmbeddedTitusGateway::getV3GrpcAgentClient).orElse(master.getV3GrpcAgentClient());
    }

    public AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient() {
        return gateway.map(EmbeddedTitusGateway::getV3BlockingGrpcAgentClient).orElse(master.getV3BlockingGrpcAgentClient());
    }

    public AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient() {
        return federation
                .map(EmbeddedTitusFederation::getAutoScaleGrpcClient)
                .orElse(gateway.map(EmbeddedTitusGateway::getAutoScaleGrpcClient).orElse(master.getAutoScaleGrpcClient()));
    }

    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        return federation
                .map(EmbeddedTitusFederation::getLoadBalancerGrpcClient)
                .orElse(gateway.map(EmbeddedTitusGateway::getLoadBalancerGrpcClient).orElse(master.getLoadBalancerGrpcClient()));
    }

    public Observable<TaskExecutorHolder> observeLaunchedTasks() {
        return master.observeLaunchedTasks();
    }

    public Observable<TaskExecutorHolder> awaitTaskExecutorHolderOf(String taskId) {
        return master.awaitTaskExecutorHolderOf(taskId);
    }
}
