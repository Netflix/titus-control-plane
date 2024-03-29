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

package com.netflix.titus.runtime.connector.jobmanager;

import javax.inject.Named;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactories;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactory;
import io.grpc.Channel;

public class JobManagerConnectorModule extends AbstractModule {

    public static final String MANAGED_CHANNEL_NAME = "ManagedChannel";

    public static final String KEEP_ALIVE_ENABLED = "keepAliveEnabled";

    private final String clientName;

    public JobManagerConnectorModule(String clientName) {
        this.clientName = clientName;
    }

    @Override
    protected void configure() {
        bind(JobDataReplicator.class).toProvider(JobDataReplicatorProvider.class);
        bind(ReadOnlyJobOperations.class).to(CachedReadOnlyJobOperations.class);
    }

    @Provides
    @Singleton
    public JobManagementClient getJobManagementClient(ReactorJobManagementServiceStub stub, TitusRuntime titusRuntime) {
        return new RemoteJobManagementClient(clientName, stub, titusRuntime);
    }

    @Provides
    @Singleton
    @Named(KEEP_ALIVE_ENABLED)
    public JobManagementClient getJobManagementClient(JobConnectorConfiguration configuration,
                                                      JobManagementServiceGrpc.JobManagementServiceStub stub,
                                                      ReactorJobManagementServiceStub reactorStub,
                                                      TitusRuntime titusRuntime) {
        return new RemoteJobManagementClientWithKeepAlive(clientName, configuration, stub, reactorStub, titusRuntime);
    }

    @Provides
    @Singleton
    public ReactorJobManagementServiceStub getReactorReactorJobManagementServiceStub(GrpcToReactorClientFactory factory,
                                                                                     @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return factory.apply(JobManagementServiceGrpc.newStub(channel), ReactorJobManagementServiceStub.class, JobManagementServiceGrpc.getServiceDescriptor());
    }

    @Provides
    @Singleton
    public JobSnapshotFactory getJobSnapshotFactory(JobConnectorConfiguration configuration,
                                                    TitusRuntime titusRuntime) {
        return JobSnapshotFactories.newDefault(false,
                configuration.isKeepAliveReplicatedStreamEnabled(),
                error -> titusRuntime.getCodeInvariants().inconsistent(error),
                titusRuntime
        );
    }
}
