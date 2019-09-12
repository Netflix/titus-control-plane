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

package com.netflix.titus.supplementary.relocation.endpoint.grpc;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.framework.fit.adapter.GrpcFitInterceptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorServerFactory;
import com.netflix.titus.runtime.endpoint.common.grpc.AbstractTitusGrpcServer;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcEndpointConfiguration;
import io.grpc.ServerInterceptor;
import io.grpc.ServiceDescriptor;

@Singleton
public class TaskRelocationGrpcServer extends AbstractTitusGrpcServer {

    private final TitusRuntime titusRuntime;

    @Inject
    public TaskRelocationGrpcServer(GrpcEndpointConfiguration configuration,
                                    ReactorTaskRelocationGrpcService reactorTaskRelocationGrpcService,
                                    GrpcToReactorServerFactory reactorServerFactory,
                                    TitusRuntime titusRuntime) {
        super(configuration, reactorServerFactory.apply(TaskRelocationServiceGrpc.getServiceDescriptor(), reactorTaskRelocationGrpcService));
        this.titusRuntime = titusRuntime;
    }

    @Override
    protected List<ServerInterceptor> createInterceptors(ServiceDescriptor serviceDescriptor) {
        return GrpcFitInterceptor.appendIfFitEnabled(super.createInterceptors(serviceDescriptor), titusRuntime);
    }
}
