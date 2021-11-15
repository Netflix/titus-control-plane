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

import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cell.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;

public class EmbeddedCellTitusOperations implements EmbeddedTitusOperations {

    private final EmbeddedTitusMaster master;
    private final Optional<EmbeddedTitusGateway> gateway;
    private final EmbeddedKubeCluster kubeCluster;

    public EmbeddedCellTitusOperations(EmbeddedTitusMaster master) {
        this(master, null);
    }

    public EmbeddedCellTitusOperations(EmbeddedTitusMaster master, EmbeddedTitusGateway gateway) {
        this.master = master;
        this.gateway = Optional.ofNullable(gateway);
        this.kubeCluster = master.getEmbeddedKubeCluster();
    }

    @Override
    public EmbeddedKubeCluster getKubeCluster() {
        return kubeCluster;
    }

    @Override
    public HealthGrpc.HealthStub getHealthClient() {
        return gateway.map(EmbeddedTitusGateway::getHealthClient).orElse(master.getHealthClient());
    }

    @Override
    public SchedulerServiceGrpc.SchedulerServiceBlockingStub getV3BlockingSchedulerClient() {
        return gateway.map(EmbeddedTitusGateway::getV3BlockingSchedulerClient).orElse(master.getV3BlockingSchedulerClient());
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
    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        return gateway.map(EmbeddedTitusGateway::getLoadBalancerGrpcClient).orElse(master.getLoadBalancerGrpcClient());
    }

    @Override
    public EvictionServiceGrpc.EvictionServiceBlockingStub getBlockingGrpcEvictionClient() {
        return gateway.map(EmbeddedTitusGateway::getBlockingGrpcEvictionClient).orElse(master.getBlockingGrpcEvictionClient());
    }
}
