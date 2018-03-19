/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.model.sanitizer.EntitySanitizer;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.master.ApiOperations;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.endpoint.common.CellInfoResolver;
import io.netflix.titus.master.endpoint.common.ConfigurableCellInfoResolver;
import io.netflix.titus.master.endpoint.grpc.GrpcEndpointConfiguration;
import io.netflix.titus.master.endpoint.grpc.TitusMasterGrpcServer;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.GrpcTitusServiceGateway;
import io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V2GrpcTitusServiceGateway;
import io.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import io.netflix.titus.runtime.endpoint.common.LogStorageInfo;

import static io.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.RoutingGrpcTitusServiceGateway.NAME_V2_ENGINE_GATEWAY;

public class EndpointModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TitusMasterGrpcServer.class).asEagerSingleton();
        bind(CellInfoResolver.class).to(ConfigurableCellInfoResolver.class);
    }

    @Provides
    @Singleton
    public GrpcEndpointConfiguration getGrpcEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcEndpointConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(NAME_V2_ENGINE_GATEWAY)
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
