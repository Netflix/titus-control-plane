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

package com.netflix.titus.supplementary.relocation.startup;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.client.GrpcAgentManagementClient;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.eviction.client.GrpcEvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.connector.jobmanager.client.GrpcJobManagementClient;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterConnectorModule;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleCallMetadataResolverProvider;

public class MasterConnectorModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new TitusMasterConnectorModule());
        bind(CallMetadataResolver.class).toProvider(SimpleCallMetadataResolverProvider.class);
        bind(AgentManagementClient.class).to(GrpcAgentManagementClient.class);
        bind(JobManagementClient.class).to(GrpcJobManagementClient.class);
        bind(EvictionServiceClient.class).to(GrpcEvictionServiceClient.class);
    }

    @Provides
    @Singleton
    public GrpcClientConfiguration getGrpcClientConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcClientConfiguration.class);
    }
}
