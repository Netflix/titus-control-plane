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

package com.netflix.titus.master.endpoint;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.master.ApiOperations;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.master.endpoint.grpc.TitusMasterGrpcServer;
import com.netflix.titus.master.job.V2JobOperations;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.RoutingGrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V2GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

public class EndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TitusMasterGrpcServer.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public GrpcEndpointConfiguration getGrpcEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcEndpointConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(RoutingGrpcTitusServiceGateway.NAME_V2_ENGINE_GATEWAY)
    public GrpcTitusServiceGateway getV2TitusServiceGateway(
            MasterConfiguration configuration,
            V2JobOperations v2JobOperations,
            JobSubmitLimiter jobSubmitLimiter,
            ApiOperations apiOperations,
            RxEventBus eventBus,
            LogStorageInfo<V2WorkerMetadata> v2LogStorage,
            @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer) {
        return new V2GrpcTitusServiceGateway(configuration, v2JobOperations, jobSubmitLimiter, apiOperations, eventBus, v2LogStorage, entitySanitizer);
    }
}
