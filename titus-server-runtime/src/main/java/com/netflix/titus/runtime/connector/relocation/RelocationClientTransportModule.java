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
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

public class RelocationClientTransportModule extends AbstractModule {

    public static final String RELOCATION_CLIENT = "relocationClient";

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    @Named(RELOCATION_CLIENT)
    public GrpcClientConfiguration getGrpcClientConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcClientConfiguration.class, "titus.relocation.grpcClient");
    }

    @Provides
    @Singleton
    @Named(RELOCATION_CLIENT)
    Channel managedChannel(@Named(RELOCATION_CLIENT) GrpcClientConfiguration configuration) {
        return NettyChannelBuilder
                .forAddress(configuration.getHostname(), configuration.getGrpcPort())
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }
}
