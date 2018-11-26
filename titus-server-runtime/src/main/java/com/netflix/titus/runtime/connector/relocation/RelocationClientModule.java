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

import java.time.Duration;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc.TaskRelocationServiceStub;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.common.react.ReactToGrpcClientBuilder;
import com.netflix.titus.runtime.connector.relocation.client.GrpcAdapterRelocationServiceClient;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.Channel;

import static com.netflix.titus.runtime.connector.relocation.RelocationClientTransportModule.RELOCATION_CLIENT;

public class RelocationClientModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(RelocationServiceClient.class).to(GrpcAdapterRelocationServiceClient.class);
    }

    @Provides
    @Singleton
    public TaskRelocationServiceStub getTaskRelocationServiceStub(@Named(RELOCATION_CLIENT) Channel channel) {
        return TaskRelocationServiceGrpc.newStub(channel);
    }

    @Provides
    @Singleton
    public ProtobufRelocationServiceClient getProtobufRelocationServiceClient(TaskRelocationServiceStub stub,
                                                                              CallMetadataResolver metadataResolver,
                                                                              @Named(RELOCATION_CLIENT) GrpcClientConfiguration configuration) {
        return ReactToGrpcClientBuilder
                .newBuilder(
                        ProtobufRelocationServiceClient.class, stub, TaskRelocationServiceGrpc.getServiceDescriptor()
                )
                .withCallMetadataResolver(metadataResolver)
                .withTimeout(Duration.ofMillis(configuration.getRequestTimeout()))
                .build();
    }
}
