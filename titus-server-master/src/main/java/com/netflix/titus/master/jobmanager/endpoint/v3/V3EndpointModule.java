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

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.DefaultJobManagementServiceGrpc;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V3GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

public class V3EndpointModule extends AbstractModule {

    public static final TypeLiteral<LogStorageInfo<Task>> V3_LOG_STORAGE_INFO =
            new TypeLiteral<LogStorageInfo<Task>>() {
            };

    @Override
    protected void configure() {
        bind(JobManagementServiceImplBase.class).to(DefaultJobManagementServiceGrpc.class);
    }

    @Provides
    @Singleton
    public GrpcTitusServiceGateway getV3ServiceGateway(V3JobOperations jobOperations,
                                                       JobSubmitLimiter jobSubmitLimiter,
                                                       LogStorageInfo<Task> v3LogStorage,
                                                       @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                                       TitusRuntime titusRuntime) {
        return new V3GrpcTitusServiceGateway(jobOperations, jobSubmitLimiter, v3LogStorage, entitySanitizer, titusRuntime);
    }
}
