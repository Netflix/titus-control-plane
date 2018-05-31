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

package com.netflix.titus.master.health.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.Details;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.master.health.service.HealthService;
import io.grpc.stub.StreamObserver;

@Singleton
public class DefaultHealthServiceGrpc extends HealthGrpc.HealthImplBase {
    private final HealthService healthService;

    @Inject
    public DefaultHealthServiceGrpc(HealthService healthService) {
        this.healthService = healthService;
    }

    @Override
    public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        Details details = healthService.getServerStatus();
        responseObserver.onNext(HealthCheckResponse.newBuilder()
                .setStatus(details.getStatus())
                .addDetails(HealthCheckResponse.ServerStatus.newBuilder()
                        .setDetails(details)
                        .build())
                .build());
        responseObserver.onCompleted();
    }
}
