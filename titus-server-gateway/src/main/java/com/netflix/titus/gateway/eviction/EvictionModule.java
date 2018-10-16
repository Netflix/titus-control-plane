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

package com.netflix.titus.gateway.eviction;

import com.google.inject.AbstractModule;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.eviction.client.GrpcEvictionServiceClient;

public class EvictionModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(EvictionServiceGrpc.EvictionServiceImplBase.class).to(GatewayGrpcEvictionService.class);
        bind(EvictionServiceClient.class).to(GrpcEvictionServiceClient.class);
    }
}
