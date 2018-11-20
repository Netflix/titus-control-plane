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

package com.netflix.titus.testkit.perf.load;

import java.util.Date;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceBlockingStub;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.eviction.client.GrpcEvictionServiceClient;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapterFactory;
import com.netflix.titus.testkit.rx.RxGrpcJobManagementService;
import io.grpc.ManagedChannel;

@Singleton
public class ExecutionContext {

    public static final String LABEL_SESSION = "titus.load.session";

    private final String sessionId;
    private final RxGrpcJobManagementService jobManagementClient;
    private final JobManagementServiceBlockingStub jobManagementClientBlocking;
    private final GrpcEvictionServiceClient evictionServiceClient;

    @Inject
    public ExecutionContext(ManagedChannel titusGrpcChannel,
                            ReactorGrpcClientAdapterFactory grpcClientAdapterFactory) {
        this.jobManagementClient = new RxGrpcJobManagementService(titusGrpcChannel);
        this.jobManagementClientBlocking = JobManagementServiceGrpc.newBlockingStub(titusGrpcChannel);
        this.evictionServiceClient = new GrpcEvictionServiceClient(
                grpcClientAdapterFactory,
                EvictionServiceGrpc.newStub(titusGrpcChannel)
        );
        this.sessionId = "startedAt=" + new Date();
    }

    public RxGrpcJobManagementService getJobManagementClient() {
        return jobManagementClient;
    }

    public EvictionServiceClient getEvictionServiceClient() {
        return evictionServiceClient;
    }

    public JobManagementServiceBlockingStub getJobManagementClientBlocking() {
        return jobManagementClientBlocking;
    }

    public String getSessionId() {
        return sessionId;
    }
}
