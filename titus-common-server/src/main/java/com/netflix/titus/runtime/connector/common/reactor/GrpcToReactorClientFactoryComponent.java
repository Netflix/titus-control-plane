/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.common.reactor;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.GrpcRequestConfigurationBean;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CommonCallMetadataUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class GrpcToReactorClientFactoryComponent {

    @Bean
    public GrpcRequestConfiguration getChannelTunablesConfiguration(Environment environment) {
        return new GrpcRequestConfigurationBean(environment);
    }

    @Bean
    public GrpcToReactorClientFactory getReactorGrpcClientAdapterFactory(GrpcRequestConfiguration configuration,
                                                                         CallMetadataResolver callMetadataResolver) {

        return new DefaultGrpcToReactorClientFactory(
                configuration,
                CommonCallMetadataUtils.newGrpcStubDecorator(callMetadataResolver),
                CallMetadata.class
        );
    }
}
