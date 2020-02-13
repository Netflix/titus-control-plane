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

package com.netflix.titus.common.util.grpc.reactor.server;

import java.util.function.Supplier;

import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;

import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

class GrpcToReactorServerBuilder<REACT_SERVICE, CONTEXT> {

    private final ServiceDescriptor serviceDescriptor;
    private final REACT_SERVICE reactorService;
    private Class<REACT_SERVICE> reactorDetailedFallbackClass;
    private Class<CONTEXT> contextType;
    private Supplier<CONTEXT> contextResolver;


    private GrpcToReactorServerBuilder(ServiceDescriptor serviceDescriptor, REACT_SERVICE reactorService) {
        this.serviceDescriptor = serviceDescriptor;
        this.reactorService = reactorService;
    }

    GrpcToReactorServerBuilder<REACT_SERVICE, CONTEXT> withContext(Class<CONTEXT> contextType, Supplier<CONTEXT> contextResolver) {
        this.contextType = contextType;
        this.contextResolver = contextResolver;
        return this;
    }

    GrpcToReactorServerBuilder<REACT_SERVICE, CONTEXT> withReactorFallbackClass(Class<REACT_SERVICE> reactorDetailedFallbackClass) {
        this.reactorDetailedFallbackClass = reactorDetailedFallbackClass;
        return this;
    }

    static <REACT_SERVICE, CONTEXT> GrpcToReactorServerBuilder<REACT_SERVICE, CONTEXT> newBuilder(
            ServiceDescriptor grpcServiceDescriptor, REACT_SERVICE reactService) {
        return new GrpcToReactorServerBuilder<>(grpcServiceDescriptor, reactService);
    }

    ServerServiceDefinition build() {
        MethodHandlersBuilder<CONTEXT, REACT_SERVICE> handlersBuilder = new MethodHandlersBuilder<>(reactorService, serviceDescriptor, contextType, contextResolver, reactorDetailedFallbackClass);

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
