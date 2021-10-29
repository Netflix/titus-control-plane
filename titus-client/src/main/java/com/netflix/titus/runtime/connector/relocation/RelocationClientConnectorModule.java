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

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.runtime.connector.relocation.noop.NoOpRelocationServiceClient;
import io.grpc.Channel;

public class RelocationClientConnectorModule extends AbstractModule {

    public static final String RELOCATION_CLIENT = "relocationClient";

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public RelocationConnectorConfiguration getRelocationConnectorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(RelocationConnectorConfiguration.class);
    }

    @Provides
    @Singleton
    public RelocationServiceClient getRelocationServiceClient(RelocationConnectorConfiguration configuration,
                                                              Injector injector,
                                                              GrpcToReactorClientFactory factory) {
        if (configuration.isEnabled()) {
            Channel channel = injector.getInstance(Key.get(Channel.class, Names.named(RELOCATION_CLIENT)));
            ReactorRelocationServiceStub stub = factory.apply(
                    TaskRelocationServiceGrpc.newStub(channel),
                    ReactorRelocationServiceStub.class,
                    TaskRelocationServiceGrpc.getServiceDescriptor()
            );
            return new RemoteRelocationServiceClient(stub);
        }
        return new NoOpRelocationServiceClient();
    }
}
