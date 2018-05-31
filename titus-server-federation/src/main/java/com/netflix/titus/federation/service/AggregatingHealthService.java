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

package com.netflix.titus.federation.service;

import java.util.function.BiConsumer;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.Unknown;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.service.HealthService;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import rx.Observable;

import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.NOT_SERVING;
import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.SERVING;
import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.UNKNOWN;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class AggregatingHealthService implements HealthService {
    private final AggregatingCellClient client;
    private final CallMetadataResolver callMetadataResolver;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingHealthService(AggregatingCellClient client, CallMetadataResolver callMetadataResolver, GrpcConfiguration grpcConfiguration) {
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
        this.grpcConfiguration = grpcConfiguration;
    }

    private ClientCall<HealthCheckResponse> check() {
        return (client, responseObserver) -> wrap(client).check(HealthCheckRequest.newBuilder().build(), responseObserver);
    }

    @Override
    public Observable<HealthCheckResponse> check(HealthCheckRequest request) {
        return client.callExpectingErrors(HealthGrpc::newStub, check())
                .map(resp -> resp.getResult().onErrorGet(e -> newErrorResponse(resp.getCell().getName(), e)))
                .reduce((one, other) -> HealthCheckResponse.newBuilder()
                        .setStatus(merge(one.getStatus(), other.getStatus()))
                        .addAllDetails(one.getDetailsList())
                        .addAllDetails(other.getDetailsList())
                        .build())
                .defaultIfEmpty(HealthCheckResponse.newBuilder()
                        .setStatus(UNKNOWN)
                        .build());
    }

    private static ServingStatus merge(ServingStatus oneStatus, ServingStatus otherStatus) {
        if (oneStatus.equals(SERVING)) {
            return otherStatus;
        }
        return oneStatus;
    }

    private static HealthCheckResponse newErrorResponse(String cellName, Throwable e) {
        return HealthCheckResponse.newBuilder()
                .setStatus(NOT_SERVING)
                .addDetails(HealthCheckResponse.ServerStatus.newBuilder()
                        .setError(Unknown.newBuilder()
                                .setCell(cellName)
                                .setErrorCode(statusCodeFromThrowable(e))
                                .setMessage(e.getMessage())
                                .build())
                        .build())
                .build();
    }

    private static String statusCodeFromThrowable(Throwable e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode().equals(Status.Code.UNKNOWN)) {
            return e.getClass().getSimpleName();
        }
        return status.getCode().toString();
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub) {
        return createWrappedStub(stub, callMetadataResolver, grpcConfiguration.getRequestTimeoutMs());
    }

    private interface ClientCall<T> extends BiConsumer<HealthGrpc.HealthStub, StreamObserver<T>> {
        // generics sanity
    }
}
