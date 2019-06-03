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

import java.util.Collections;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.MasterStatus;
import com.netflix.titus.api.supervisor.model.ServerPort;
import com.netflix.titus.api.supervisor.service.LeaderActivator;
import com.netflix.titus.api.supervisor.service.LeaderElector;
import com.netflix.titus.api.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.api.supervisor.service.MasterMonitor;
import com.netflix.titus.api.supervisor.service.SupervisorOperations;
import com.netflix.titus.api.supervisor.service.resolver.AlwaysEnabledLocalMasterReadinessResolver;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.NetworkExt;
import com.netflix.titus.master.endpoint.grpc.GrpcMasterEndpointConfiguration;
import com.netflix.titus.master.supervisor.SupervisorConfiguration;
import com.netflix.titus.master.supervisor.service.leader.GuiceLeaderActivator;
import com.netflix.titus.master.supervisor.service.leader.ImmediateLeaderElector;
import com.netflix.titus.master.supervisor.service.leader.LeaderElectionOrchestrator;
import com.netflix.titus.master.supervisor.service.leader.LocalMasterMonitor;
import com.netflix.titus.master.supervisor.service.resolver.DefaultLocalMasterInstanceResolver;

public class SupervisorServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LocalMasterReadinessResolver.class).toInstance(AlwaysEnabledLocalMasterReadinessResolver.getInstance());
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

    /**
     * As MasterInstance data contain a lot of details that are deployment specific, this binding is provided here
     * for completeness/as an example only. It should be overridden by deployment specific configuration.
     */
    @Provides
    @Singleton
    public LocalMasterInstanceResolver getLocalMasterInstanceResolver(SupervisorConfiguration configuration,
                                                                      GrpcMasterEndpointConfiguration grpcServerConfiguration,
                                                                      LocalMasterReadinessResolver localMasterReadinessResolver,
                                                                      TitusRuntime titusRuntime) {
        String ipAddress = NetworkExt.getLocalIPs().flatMap(ips -> ips.stream().filter(NetworkExt::isIpV4).findFirst()).orElse("127.0.0.1");

        ServerPort grpcPort = ServerPort.newBuilder()
                .withPortNumber(grpcServerConfiguration.getPort())
                .withSecure(false)
                .withProtocol("grpc")
                .withDescription("TitusMaster GRPC endpoint")
                .build();

        MasterInstance initial = MasterInstance.newBuilder()
                .withInstanceId(configuration.getTitusMasterInstanceId())
                .withInstanceGroupId(configuration.getTitusMasterInstanceId() + "Group")
                .withIpAddress(ipAddress)
                .withStatusHistory(Collections.emptyList())
                .withStatus(MasterStatus.newBuilder()
                        .withState(MasterState.Starting)
                        .withMessage("Bootstrapping")
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build()
                )
                .withServerPorts(Collections.singletonList(grpcPort))
                .build();
        return new DefaultLocalMasterInstanceResolver(localMasterReadinessResolver, initial);
    }
}
