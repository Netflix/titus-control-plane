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

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.loadshedding.AdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionControllers;
import com.netflix.titus.common.util.loadshedding.grpc.GrpcAdmissionControllerServerInterceptor;
import com.netflix.titus.master.endpoint.grpc.GrpcMasterEndpointConfiguration;
import com.netflix.titus.master.endpoint.grpc.TitusMasterGrpcServer;
import com.netflix.titus.runtime.endpoint.authorization.AuthorizationServiceModule;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveModule;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;

public class MasterEndpointModule extends AbstractModule {

    public static final String GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX = "titus.master.grpcServer.admissionController.buckets";

    private static final String UNIDENTIFIED = "unidentified";

    @Override
    protected void configure() {
        bind(TitusMasterGrpcServer.class).asEagerSingleton();
        install(new CallMetadataResolveModule());
        install(new AuthorizationServiceModule());
    }

    @Provides
    @Singleton
    public GrpcMasterEndpointConfiguration getGrpcEndpointConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcMasterEndpointConfiguration.class);
    }

    @Provides
    @Singleton
    public GrpcAdmissionControllerServerInterceptor getGrpcAdmissionControllerServerInterceptor(Config config,
                                                                                                GrpcMasterEndpointConfiguration configuration,
                                                                                                CallMetadataResolver callMetadataResolver,
                                                                                                TitusRuntime titusRuntime) {
        AdmissionController mainController = AdmissionControllers.tokenBucketsFromArchaius(
                config.getPrefixedView(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX),
                titusRuntime
        );
        AdmissionController circuitBreaker = AdmissionControllers.circuitBreaker(
                mainController,
                configuration::isAdmissionControllerEnabled
        );
        AdmissionController spectator = AdmissionControllers.spectator(circuitBreaker, titusRuntime);

        return new GrpcAdmissionControllerServerInterceptor(
                spectator,
                () -> callMetadataResolver.resolve().map(c -> {
                    if (CollectionsExt.isNullOrEmpty(c.getCallers())) {
                        return UNIDENTIFIED;
                    }
                    return c.getCallers().get(0).getId();
                }).orElse(UNIDENTIFIED)
        );
    }
}
