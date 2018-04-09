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
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CellWithFailingLoadBalancers extends LoadBalancerServiceGrpc.LoadBalancerServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(CellWithFailingLoadBalancers.class);
    private Status errorStatus;

    CellWithFailingLoadBalancers(Status errorStatus) {
        this.errorStatus = errorStatus;
    }

    @Override
    public void getJobLoadBalancers(JobId request, StreamObserver<GetJobLoadBalancersResult> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }

    @Override
    public void getAllLoadBalancers(GetAllLoadBalancersRequest request, StreamObserver<GetAllLoadBalancersResult> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }

    @Override
    public void addLoadBalancer(AddLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }

    @Override
    public void removeLoadBalancer(RemoveLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        responseObserver.onError(errorStatus.asRuntimeException());
    }
}
