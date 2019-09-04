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

import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;

public class DefaultGrpcToReactorServerFactory implements GrpcToReactorServerFactory {

    private final CallMetadataResolver callMetadataResolver;

    public DefaultGrpcToReactorServerFactory(CallMetadataResolver callMetadataResolver) {
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public <REACT_SERVICE> ServerServiceDefinition apply(ServiceDescriptor serviceDescriptor, REACT_SERVICE reactService) {
        return GrpcToReactorServerBuilder
                .newBuilder(serviceDescriptor, reactService)
                .withCallMetadataResolver(callMetadataResolver)
                .build();
    }
}
