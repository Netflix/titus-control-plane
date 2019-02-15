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

package com.netflix.titus.runtime.connector.eviction;

import javax.inject.Named;

import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.runtime.connector.common.reactor.ReactorToGrpcClientFactory;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterConnectorModule;
import io.grpc.Channel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EvictionConnectorComponent {

    @Bean
    public ReactorEvictionServiceStub getReactorEvictionServiceStub(ReactorToGrpcClientFactory factory,
                                                                    @Named(TitusMasterConnectorModule.MANAGED_CHANNEL_NAME) Channel channel) {
        return factory.apply(EvictionServiceGrpc.newStub(channel), ReactorEvictionServiceStub.class);
    }
}
