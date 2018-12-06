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

package com.netflix.titus.testkit.perf.load;

import com.google.inject.AbstractModule;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.DefaultReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;

public class LoadModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ReactorGrpcClientAdapterFactory.class).to(DefaultReactorGrpcClientAdapterFactory.class);
        bind(GrpcClientConfiguration.class).toInstance(new GrpcClientConfiguration() {
            @Override
            public String getHostname() {
                return "localhost";
            }

            @Override
            public int getGrpcPort() {
                return 123;
            }

            @Override
            public long getRequestTimeout() {
                return 30_000;
            }

            @Override
            public int getMaxTaskPageSize() {
                return 1000;
            }
        });
        bind(CallMetadataResolver.class).to(AnonymousCallMetadataResolver.class);
    }
}
