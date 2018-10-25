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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.AbstractStub;

@Singleton
public class DefaultReactorGrpcClientAdapterFactory implements ReactorGrpcClientAdapterFactory {

    private final GrpcClientConfiguration configuration;
    private final CallMetadataResolver callMetadataResolver;

    private final ConcurrentMap<AbstractStub, ReactorGrpcClientAdapter> adapters = new ConcurrentHashMap<>();

    @Inject
    public DefaultReactorGrpcClientAdapterFactory(GrpcClientConfiguration configuration,
                                                  CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public <CLIENT extends AbstractStub<CLIENT>> ReactorGrpcClientAdapter<CLIENT> newAdapter(CLIENT client) {
        return adapters.computeIfAbsent(client, c -> new ReactorGrpcClientAdapter<>(client, callMetadataResolver, configuration));
    }
}
