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

package com.netflix.titus.runtime.connector.relocation;

import javax.inject.Named;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorClientFactory;
import io.grpc.Channel;

public class RelocationClientConnectorModule extends AbstractModule {

    public static final String RELOCATION_CLIENT = "relocationClient";

    @Override
    protected void configure() {
        bind(RelocationServiceClient.class).to(RemoteRelocationServiceClient.class);
    }

    @Provides
    @Singleton
    public ReactorRelocationServiceStub getReactorEvictionServiceStub(GrpcToReactorClientFactory factory,
                                                                      @Named(RELOCATION_CLIENT) Channel channel) {
        return factory.apply(TaskRelocationServiceGrpc.newStub(channel), ReactorRelocationServiceStub.class, TaskRelocationServiceGrpc.getServiceDescriptor());
    }
}
