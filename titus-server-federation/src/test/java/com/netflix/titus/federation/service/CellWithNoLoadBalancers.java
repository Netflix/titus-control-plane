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

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.*;
import io.grpc.stub.StreamObserver;

/**
 * Testing class that returns empty responses to each call.
 */
public class CellWithNoLoadBalancers extends LoadBalancerServiceGrpc.LoadBalancerServiceImplBase {
    @Override
    public void getJobLoadBalancers(JobId request, StreamObserver<GetJobLoadBalancersResult> responseObserver) {
        responseObserver.onNext(GetJobLoadBalancersResult.newBuilder()
                .setJobId(request.getId())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAllLoadBalancers(GetAllLoadBalancersRequest request, StreamObserver<GetAllLoadBalancersResult> responseObserver) {
        responseObserver.onNext(GetAllLoadBalancersResult.newBuilder()
                .setPagination(Pagination.newBuilder()
                        .setHasMore(false)
                        .setTotalItems(0)
                        .setTotalPages(0)
                        .setCursor("")
                        .setCursorPosition(0)
                        .build())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void addLoadBalancer(AddLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeLoadBalancer(RemoveLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
