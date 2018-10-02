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

package com.netflix.titus.master.agent;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.agent.store.AgentStore;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.master.agent.service.AgentManagementConfiguration;
import com.netflix.titus.master.agent.service.DefaultAgentManagementService;
import com.netflix.titus.master.agent.service.cache.AgentCache;
import com.netflix.titus.master.agent.service.cache.DefaultAgentCache;
import com.netflix.titus.master.agent.service.monitor.AgentMonitorConfiguration;
import com.netflix.titus.master.agent.service.monitor.LifecycleAgentStatusMonitor;
import com.netflix.titus.master.agent.service.monitor.OnOffStatusMonitor;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import com.netflix.titus.master.agent.store.AgentStoreReaper;
import com.netflix.titus.master.agent.store.AgentStoreReaperConfiguration;
import com.netflix.titus.master.agent.store.InMemoryAgentStore;
import com.netflix.titus.master.scheduler.VmOperationsInstanceCloudConnector;
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
    public AgentStoreReaperConfiguration getAgentStoreReaperConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(AgentStoreReaperConfiguration.class);
    }

    @Provides
    @Singleton
    public AgentStatusMonitor getAgentStatusMonitor(AgentManagementService agentManagementService,
                                                    LifecycleAgentStatusMonitor lifecycleAgentStatusMonitor,
                                                    AgentMonitorConfiguration config) {
        return new OnOffStatusMonitor(agentManagementService, lifecycleAgentStatusMonitor, config::isLifecycleStatusMonitorEnabled, Schedulers.computation());
    }
}
