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

package com.netflix.titus.testkit.embedded.cloud.endpoint;

import com.google.inject.AbstractModule;
import com.netflix.titus.simulator.SimulatedAgentServiceGrpc.SimulatedAgentServiceImplBase;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc;
import com.netflix.titus.testkit.embedded.cloud.endpoint.grpc.GrpcSimulatedAgentsService;
import com.netflix.titus.testkit.embedded.cloud.endpoint.grpc.GrpcSimulatedMesosService;
import com.netflix.titus.testkit.embedded.cloud.endpoint.grpc.SimulatedCloudGrpcServer;

public class SimulatedCloudEndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(SimulatedAgentServiceImplBase.class).to(GrpcSimulatedAgentsService.class);
        bind(SimulatedMesosServiceGrpc.SimulatedMesosServiceImplBase.class).to(GrpcSimulatedMesosService.class);
        bind(SimulatedCloudGrpcServer.class).asEagerSingleton();
    }
}
