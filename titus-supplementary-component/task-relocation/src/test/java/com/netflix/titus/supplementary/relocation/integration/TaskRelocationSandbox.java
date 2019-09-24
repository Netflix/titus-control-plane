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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.titus.runtime.clustermembership.connector.ClusterMembershipInMemoryConnectorComponent;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipGrpcEndpointComponent;
import com.netflix.titus.runtime.clustermembership.service.ClusterMembershipServiceComponent;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationComponent;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorServerFactoryComponent;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveComponent;
import com.netflix.titus.runtime.health.AlwaysHealthyComponent;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.RelocationLeaderActivator;
import com.netflix.titus.supplementary.relocation.descheduler.DeschedulerComponent;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcComponent;
import com.netflix.titus.supplementary.relocation.endpoint.grpc.TaskRelocationGrpcServerRunner;
import com.netflix.titus.supplementary.relocation.endpoint.rest.TaskRelocationExceptionHandler;
import com.netflix.titus.supplementary.relocation.endpoint.rest.TaskRelocationSpringResource;
import com.netflix.titus.supplementary.relocation.store.memory.InMemoryRelocationStoreComponent;
import com.netflix.titus.supplementary.relocation.workflow.TaskRelocationWorkflowComponent;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.env.MockEnvironment;

/**
 * Task relocation server runner, with stubbed external connectors. Used by the task relocation service integration tests.
 */
public class TaskRelocationSandbox {

    private final AnnotationConfigApplicationContext container;
    private final BlockingQueue<ManagedChannel> channels = new LinkedBlockingQueue<>();

    public TaskRelocationSandbox(RelocationConnectorStubs relocationConnectorStubs) {
        MockEnvironment config = new MockEnvironment();
        config.setProperty("titus.relocation.endpoint.port", "0");
        config.setProperty("titus.relocation.relocationScheduleIntervalMs", "100");
        config.setProperty("titus.relocation.deschedulingIntervalMs", "100");
        config.setProperty("titus.relocation.relocationTimeoutMs", "60000");
        config.setProperty("titus.relocation.dataStalenessThresholdMs", "30000");

        this.container = new AnnotationConfigApplicationContext();
        container.getEnvironment().merge(config);
        container.setParent(relocationConnectorStubs.getApplicationContext());

        container.register(AlwaysHealthyComponent.class);
        container.register(ClusterMembershipInMemoryConnectorComponent.class);
        container.register(ClusterMembershipServiceComponent.class);
        container.register(ClusterMembershipGrpcEndpointComponent.class);
        container.register(LeaderActivationComponent.class);

        container.register(CallMetadataResolveComponent.class);
        container.register(GrpcToReactorServerFactoryComponent.class);
        container.register(RelocationConfiguration.class);
        container.register(InMemoryRelocationStoreComponent.class);
        container.register(DeschedulerComponent.class);
        container.register(TaskRelocationWorkflowComponent.class);
        container.register(TaskRelocationGrpcComponent.class);
        container.register(TaskRelocationGrpcServerRunner.class);
        container.register(TaskRelocationSpringResource.class);
        container.register(TaskRelocationExceptionHandler.class);
        container.register(RelocationLeaderActivator.class);
        container.refresh();
        container.start();
    }

    public void shutdown() {
        for (ManagedChannel channel : channels) {
            channel.shutdownNow();
        }
        container.close();
    }

    public ManagedChannel getGrpcChannel() {
        int port = container.getBean(TaskRelocationGrpcServerRunner.class).getServer().getPort();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext(true)
                .build();
        channels.add(channel);
        return channel;
    }
}
