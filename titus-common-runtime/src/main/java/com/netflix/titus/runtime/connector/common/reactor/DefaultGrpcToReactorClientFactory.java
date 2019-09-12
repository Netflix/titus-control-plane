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
import java.util.Optional;
import java.util.function.BiFunction;

import com.netflix.titus.common.util.grpc.reactor.client.ReactorToGrpcClientBuilder;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;

public class DefaultGrpcToReactorClientFactory<CONTEXT> implements GrpcToReactorClientFactory {

    private final GrpcRequestConfiguration configuration;
    private final BiFunction grpcStubDecorator;
    private final Class<CONTEXT> contextType;

    public DefaultGrpcToReactorClientFactory(GrpcRequestConfiguration configuration,
                                             BiFunction<AbstractStub, Optional<CONTEXT>, AbstractStub> grpcStubDecorator,
                                             Class<CONTEXT> contextType) {
        this.configuration = configuration;
        this.grpcStubDecorator = grpcStubDecorator;
        this.contextType = contextType;
    }

    @Override
    public <GRPC_STUB extends AbstractStub<GRPC_STUB>, REACT_API> REACT_API apply(GRPC_STUB stub, Class<REACT_API> apiType, ServiceDescriptor serviceDescriptor) {
        return ReactorToGrpcClientBuilder
                .newBuilder(
                        apiType, stub, serviceDescriptor, contextType
                )
                .withGrpcStubDecorator((BiFunction<GRPC_STUB, Optional<CONTEXT>, GRPC_STUB>) grpcStubDecorator)
                .withTimeout(Duration.ofMillis(configuration.getRequestTimeoutMs()))
                .withStreamingTimeout(Duration.ofMillis(configuration.getStreamingTimeoutMs()))
                .build();
    }
}
