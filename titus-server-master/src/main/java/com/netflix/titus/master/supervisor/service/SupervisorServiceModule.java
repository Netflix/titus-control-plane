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

package com.netflix.titus.master.supervisor.service;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.supervisor.service.LeaderActivator;
import com.netflix.titus.api.supervisor.service.LeaderElector;
import com.netflix.titus.api.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.api.supervisor.service.MasterMonitor;
import com.netflix.titus.api.supervisor.service.SupervisorOperations;
import com.netflix.titus.master.supervisor.SupervisorConfiguration;
import com.netflix.titus.master.supervisor.service.leader.GuiceLeaderActivator;
import com.netflix.titus.master.supervisor.service.leader.ImmediateLeaderElector;
import com.netflix.titus.master.supervisor.service.leader.ImmediateLocalMasterInstanceResolver;
import com.netflix.titus.master.supervisor.service.leader.LeaderElectionOrchestrator;
import com.netflix.titus.master.supervisor.service.leader.LocalMasterMonitor;

public class SupervisorServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalMasterInstanceResolver.class).to(ImmediateLocalMasterInstanceResolver.class);
        bind(MasterMonitor.class).to(LocalMasterMonitor.class);
        bind(LeaderElector.class).to(ImmediateLeaderElector.class).asEagerSingleton();
        bind(LeaderActivator.class).to(GuiceLeaderActivator.class);
        bind(LeaderElectionOrchestrator.class).asEagerSingleton();
        bind(SupervisorOperations.class).to(DefaultSupervisorOperations.class);
    }

    @Provides
    @Singleton
    public SupervisorConfiguration getSupervisorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(SupervisorConfiguration.class);
    }
}
