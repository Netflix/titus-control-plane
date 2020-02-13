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

import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;

public class DefaultGrpcToReactorServerFactory<CONTEXT> implements GrpcToReactorServerFactory {

    private final Class<CONTEXT> contextType;
    private final Supplier<CONTEXT> contextResolver;

    public DefaultGrpcToReactorServerFactory(Class<CONTEXT> contextType, Supplier<CONTEXT> contextResolver) {
        this.contextType = contextType;
        this.contextResolver = contextResolver;
    }

    @Override
    public <REACT_SERVICE> ServerServiceDefinition apply(ServiceDescriptor serviceDescriptor, REACT_SERVICE reactService) {
        return apply(serviceDescriptor, reactService, (Class<REACT_SERVICE>) reactService.getClass());
    }

    @Override
    public <REACT_SERVICE> ServerServiceDefinition apply(ServiceDescriptor serviceDescriptor, REACT_SERVICE reactService, Class<REACT_SERVICE> reactorDetailedFallbackClass) {
        return GrpcToReactorServerBuilder.<REACT_SERVICE, CONTEXT>newBuilder(serviceDescriptor, reactService)
                .withContext(contextType, contextResolver)
                .withReactorFallbackClass(reactorDetailedFallbackClass)
                .build();
    }
}
