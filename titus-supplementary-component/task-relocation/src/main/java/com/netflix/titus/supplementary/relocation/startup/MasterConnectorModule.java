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
