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

package com.netflix.titus.supplementary.relocation.integration;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.config.MapConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.descheduler.DeschedulerModule;
import com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationEndpointModule;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcServer;
import com.netflix.titus.supplementary.relocation.store.memory.InMemoryRelocationStoreModule;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowModule;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Task relocation server runner, with stubbed external connectors. Used by the task relocation service integration tests.
 */
public class TaskRelocationSandbox {

    private final LifecycleInjector injector;

    public TaskRelocationSandbox(RelocationConnectorStubs relocationConnectorStubs) {
        this.injector = InjectorBuilder.fromModules(
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindDefaultConfig().toInstance(MapConfig.builder()
                                .put("titus.relocation.relocationScheduleIntervalMs", "100")
                                .put("titus.relocation.deschedulingIntervalMs", "100")
                                .put("titus.relocation.endpoint.port", "0")
                                .build()
                        );
                    }
                },
                new AbstractModule() {
                    @Override
                    protected void configure() {
                    }

                    @Provides
                    @Singleton
                    public RelocationConfiguration getRelocationConfiguration(ConfigProxyFactory factory) {
                        return factory.newProxy(RelocationConfiguration.class);
                    }
                },
                relocationConnectorStubs.getModule(),
                new InMemoryRelocationStoreModule(),
                new DeschedulerModule(),
                new RelocationWorkflowModule(),
                new TaskRelocationEndpointModule()
        ).createInjector();
    }

    public void shutdown() {
        injector.close();
    }

    public ManagedChannel getGrpcChannel() {
        int port = injector.getInstance(TaskRelocationGrpcServer.class).getPort();
        return ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext(true)
                .build();
    }
}
