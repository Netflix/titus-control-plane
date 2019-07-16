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

package com.netflix.titus.runtime.connector.common.reactor.server;

import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;

import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

class GrpcToReactorServerBuilder<REACT_SERVICE> {

    private final ServiceDescriptor serviceDescriptor;
    private final REACT_SERVICE reactorService;

    private CallMetadataResolver callMetadataResolver;

    private GrpcToReactorServerBuilder(ServiceDescriptor serviceDescriptor, REACT_SERVICE reactorService) {
        this.serviceDescriptor = serviceDescriptor;
        this.reactorService = reactorService;
    }

    GrpcToReactorServerBuilder<REACT_SERVICE> withCallMetadataResolver(CallMetadataResolver callMetadataResolver) {
        this.callMetadataResolver = callMetadataResolver;
        return this;
    }

    static <REACT_SERVICE> GrpcToReactorServerBuilder<REACT_SERVICE> newBuilder(
            ServiceDescriptor grpcServiceDescriptor, REACT_SERVICE reactService) {
        return new GrpcToReactorServerBuilder<>(grpcServiceDescriptor, reactService);
    }

    ServerServiceDefinition build() {
        MethodHandlersBuilder handlersBuilder = new MethodHandlersBuilder(reactorService, serviceDescriptor, callMetadataResolver);

        ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(serviceDescriptor);
        handlersBuilder.getUnaryMethodHandlers().forEach(handler -> {
            builder.addMethod(handler.getMethodDescriptor(), asyncUnaryCall(handler));
        });
        handlersBuilder.getServerStreamingMethodHandlers().forEach(handler -> {
            builder.addMethod(handler.getMethodDescriptor(), asyncServerStreamingCall(handler));
        });
        return builder.build();
    }
}
