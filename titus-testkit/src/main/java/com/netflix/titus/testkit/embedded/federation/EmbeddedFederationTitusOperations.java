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

import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;

class EmbeddedFederationTitusOperations implements EmbeddedTitusOperations {

    private final EmbeddedTitusFederation federation;
    private final EmbeddedKubeCluster kubeCluster;

    EmbeddedFederationTitusOperations(EmbeddedTitusFederation federation) {
        this.federation = federation;
        // We assume, a single cloud simulator instance is shared between all cells.
        EmbeddedTitusOperations titusOperations = this.federation.getCells().get(0).getTitusOperations();
        this.kubeCluster = titusOperations.getKubeCluster();
    }

    @Override
    public EmbeddedKubeCluster getKubeCluster() {
        return kubeCluster;
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

    @Override
    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        return federation.getLoadBalancerGrpcClient();
    }

    @Override
    public EvictionServiceGrpc.EvictionServiceBlockingStub getBlockingGrpcEvictionClient() {
        return federation.getBlockingGrpcEvictionClient();
    }
}
