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

package com.netflix.titus.master.jobmanager.endpoint.v3;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.DefaultJobManagementServiceGrpc;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultGrpcObjectsCache;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcObjectsCache;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcObjectsCacheConfiguration;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupClient;

public class V3EndpointModule extends AbstractModule {

    public static final TypeLiteral<LogStorageInfo<Task>> V3_LOG_STORAGE_INFO =
            new TypeLiteral<LogStorageInfo<Task>>() {
            };

    @Override
    protected void configure() {
        bind(GrpcObjectsCache.class).to(DefaultGrpcObjectsCache.class);
        bind(JobManagementServiceImplBase.class).to(DefaultJobManagementServiceGrpc.class);
    }

    @Provides
    @Singleton
    public GrpcObjectsCacheConfiguration getGrpcObjectsCacheConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(GrpcObjectsCacheConfiguration.class);
    }
}
