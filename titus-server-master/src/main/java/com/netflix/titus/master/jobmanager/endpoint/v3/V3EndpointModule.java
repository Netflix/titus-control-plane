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

package com.netflix.titus.master.jobmanager.endpoint.v3;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.ApiOperations;
import com.netflix.titus.master.cluster.LeaderActivator;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.master.endpoint.adapter.LegacyTitusServiceGatewayGuard;
import com.netflix.titus.master.endpoint.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.DefaultJobManagementServiceGrpc;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.RoutingGrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V2GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V3GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.master.MasterMonitor;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleGrpcCallMetadataResolver;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

public class V3EndpointModule extends AbstractModule {

    public static final TypeLiteral<LogStorageInfo<Task>> V3_LOG_STORAGE_INFO =
            new TypeLiteral<LogStorageInfo<Task>>() {
            };

    @Override
    protected void configure() {
        bind(GrpcTitusServiceGateway.class).to(V2GrpcTitusServiceGateway.class);
        bind(CallMetadataResolver.class).to(SimpleGrpcCallMetadataResolver.class);
        bind(JobManagementServiceImplBase.class).to(DefaultJobManagementServiceGrpc.class);
    }

    @Provides
    @Singleton
    @Named(RoutingGrpcTitusServiceGateway.NAME_V3_ENGINE_GATEWAY)
    public GrpcTitusServiceGateway getV3ServiceGateway(V3JobOperations jobOperations,
                                                       JobSubmitLimiter jobSubmitLimiter,
                                                       LogStorageInfo<Task> v3LogStorage,
                                                       @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                                       TitusRuntime titusRuntime) {
        return new V3GrpcTitusServiceGateway(jobOperations, jobSubmitLimiter, v3LogStorage, entitySanitizer, titusRuntime);
    }

    @Provides
    @Singleton
    public TitusServiceGateway<String, JobDescriptor, JobSpecCase, Job, com.netflix.titus.grpc.protogen.Task, TaskStatus.TaskState> getV3RoutingServiceGateway(
            @Named(RoutingGrpcTitusServiceGateway.NAME_V2_ENGINE_GATEWAY) GrpcTitusServiceGateway v2EngineGateway,
            @Named(RoutingGrpcTitusServiceGateway.NAME_V3_ENGINE_GATEWAY) GrpcTitusServiceGateway v3EngineGateway,
            AgentManagementService agentManagementService,
            ApplicationSlaManagementService capacityGroupManagement,
            ApiOperations apiOperations,
            MasterMonitor masterMonitor,
            LeaderActivator leaderActivator,
            GrpcEndpointConfiguration configuration,
            MasterConfiguration masterConfiguration,
            CellInfoResolver cellInfoResolver) {
        RoutingGrpcTitusServiceGateway serviceGateway = new RoutingGrpcTitusServiceGateway(
                v2EngineGateway, v3EngineGateway, agentManagementService, capacityGroupManagement,
                configuration, masterConfiguration, cellInfoResolver
        );
        return new LegacyTitusServiceGatewayGuard<>(serviceGateway, apiOperations, masterMonitor, leaderActivator);
    }
}
