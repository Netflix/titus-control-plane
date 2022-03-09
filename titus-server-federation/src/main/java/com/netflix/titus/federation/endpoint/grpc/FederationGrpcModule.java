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

package com.netflix.titus.federation.endpoint.grpc;

import com.google.inject.AbstractModule;
import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.runtime.endpoint.v3.grpc.*;

public class FederationGrpcModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(TitusFederationGrpcServer.class).asEagerSingleton();
        bind(HealthGrpc.HealthImplBase.class).to(DefaultHealthServiceGrpc.class);
        bind(SchedulerServiceGrpc.SchedulerServiceImplBase.class).to(AggregatingSchedulerServiceGrpc.class);
        bind(JobManagementServiceGrpc.JobManagementServiceImplBase.class).to(DefaultJobManagementServiceGrpc.class);
        bind(AutoScalingServiceGrpc.AutoScalingServiceImplBase.class).to(DefaultAutoScalingServiceGrpc.class);
        bind(LoadBalancerServiceGrpc.LoadBalancerServiceImplBase.class).to(DefaultLoadBalancerServiceGrpc.class);
        bind(JobActivityHistoryServiceGrpc.JobActivityHistoryServiceImplBase.class).to(DefaultJobActivityHistoryServiceGrpc.class);
    }
}
