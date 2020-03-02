/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.federation.endpoint;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.archaius2.SpringConfig;
import com.netflix.titus.common.util.loadshedding.AdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionControllers;
import com.netflix.titus.common.util.loadshedding.grpc.GrpcAdmissionControllerServerInterceptor;
import com.netflix.titus.federation.endpoint.grpc.FederationGrpcComponent;
import com.netflix.titus.federation.endpoint.rest.FederationRestComponent;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolverComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@Import({
        CallMetadataResolverComponent.class,
        FederationGrpcComponent.class,
        FederationRestComponent.class,
})
public class FederationEndpointComponent {

    private static final String ADMISSION_CONTROLLER_CONFIGURATION_PREFIX = "titus.federation.admissionController.buckets";
    private static final String UNIDENTIFIED = "unidentified";

    @Bean
    public EndpointConfiguration getGrpcEndpointConfiguration(Environment environment) {
        return Archaius2Ext.newConfiguration(EndpointConfiguration.class, environment);
    }

    @Bean
    public GrpcAdmissionControllerServerInterceptor getGrpcAdmissionControllerServerInterceptor(Environment environment,
                                                                                                EndpointConfiguration configuration,
                                                                                                CallMetadataResolver callMetadataResolver,
                                                                                                TitusRuntime titusRuntime) {
        AdmissionController mainController = AdmissionControllers.tokenBucketsFromArchaius(
                new SpringConfig(ADMISSION_CONTROLLER_CONFIGURATION_PREFIX, environment),
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
