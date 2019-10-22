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
import com.netflix.titus.common.util.grpc.reactor.GrpcToReactorClientFactory;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;
import io.grpc.Channel;

public class JobManagerConnectorModule extends AbstractModule {

    public static final String MANAGED_CHANNEL_NAME = "ManagedChannel";

    @Override
    protected void configure() {
        bind(JobManagementClient.class).to(RemoteJobManagementClient.class);
        bind(JobDataReplicator.class).toProvider(JobDataReplicatorProvider.class);
        bind(ReadOnlyJobOperations.class).to(CachedReadOnlyJobOperations.class);
    }

    @Provides
    @Singleton
    public ReactorJobManagementServiceStub getReactorReactorJobManagementServiceStub(GrpcToReactorClientFactory factory,
                                                                                     @Named(MANAGED_CHANNEL_NAME) Channel channel) {
        return factory.apply(JobManagementServiceGrpc.newStub(channel), ReactorJobManagementServiceStub.class, JobManagementServiceGrpc.getServiceDescriptor());
    }
}
