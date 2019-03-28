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

import java.time.Duration;

import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;

public class DefaultGrpcToReactorClientFactory implements GrpcToReactorClientFactory {

    private final GrpcRequestConfiguration configuration;
    private final CallMetadataResolver callMetadataResolver;

    public DefaultGrpcToReactorClientFactory(GrpcRequestConfiguration configuration,
                                             CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public <GRPC_STUB extends AbstractStub<GRPC_STUB>, REACT_API> REACT_API apply(GRPC_STUB stub, Class<REACT_API> apiType, ServiceDescriptor serviceDescriptor) {
        return ReactorToGrpcClientBuilder
                .newBuilder(
                        apiType, stub, serviceDescriptor
                )
                .withCallMetadataResolver(callMetadataResolver)
                .withTimeout(Duration.ofMillis(configuration.getRequestTimeoutMs()))
                .withStreamingTimeout(Duration.ofMillis(configuration.getStreamingTimeoutMs()))
                .build();
    }
}
