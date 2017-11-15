/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.agent;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.master.agent.service.AgentManagementConfiguration;
import io.netflix.titus.master.agent.service.DefaultAgentManagementService;
import io.netflix.titus.master.agent.service.monitor.AgentMonitorConfiguration;
import io.netflix.titus.master.agent.service.monitor.OnOffStatusMonitor;
import io.netflix.titus.master.agent.service.monitor.V2JobStatusMonitor;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import io.netflix.titus.master.agent.service.cache.AgentCache;
import io.netflix.titus.master.agent.service.cache.DefaultAgentCache;
import io.netflix.titus.master.agent.store.AgentStoreReaper;
import io.netflix.titus.master.agent.store.InMemoryAgentStore;
import io.netflix.titus.master.scheduler.VmOperationsInstanceCloudConnector;
import rx.schedulers.Schedulers;

public class AgentModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ServerInfoResolver.class).toInstance(ServerInfoResolvers.fromAwsInstanceTypes());

        bind(AgentManagementService.class).to(DefaultAgentManagementService.class);
        bind(AgentCache.class).to(DefaultAgentCache.class);

        bind(AgentStore.class).to(InMemoryAgentStore.class);
        bind(AgentStoreReaper.class).asEagerSingleton();

        bind(InstanceCloudConnector.class).to(VmOperationsInstanceCloudConnector.class);
    }

    @Provides
    @Singleton
    public AgentMonitorConfiguration getAgentMonitorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(AgentMonitorConfiguration.class);
    }

    @Provides
    @Singleton
    public AgentManagementConfiguration getAgentManagementConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(AgentManagementConfiguration.class);
    }

    @Provides
    @Singleton
    public AgentStatusMonitor getAgentStatusMonitor(AgentManagementService agentManagementService,
                                                    V2JobStatusMonitor v2JobStatusMonitor,
                                                    AgentMonitorConfiguration config) {
        return new OnOffStatusMonitor(agentManagementService, v2JobStatusMonitor, config::isJobStatusMonitorEnabled, Schedulers.computation());
    }
}
