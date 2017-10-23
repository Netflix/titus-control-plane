/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.loadbalancer.endpoint.grpc;

import javax.inject.Inject;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetLoadBalancerResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;

public class DefaultLoadBalancerServiceGrpc extends LoadBalancerServiceGrpc.LoadBalancerServiceImplBase {
    private final LoadBalancerService loadBalancerService;

    @Inject
    public DefaultLoadBalancerServiceGrpc(LoadBalancerService loadBalancerService) {
        this.loadBalancerService = loadBalancerService;
    }

    @Override
    public void getJobLoadBalancers(JobId request, StreamObserver<GetLoadBalancerResult> responseObserver) {
        GetLoadBalancerResult.Builder resultBuilder = GetLoadBalancerResult.newBuilder();
        loadBalancerService.getJobLoadBalancers(request.getId()).subscribe(
                loadBalancerId -> resultBuilder.addLoadBalancers(LoadBalancerId.newBuilder().setId(loadBalancerId).build()),
                responseObserver::onError,
                () -> {
                    responseObserver.onNext(resultBuilder.build());
                    responseObserver.onCompleted();
                }
        );
        super.getJobLoadBalancers(request, responseObserver);
    }

    @Override
    public void addLoadBalancer(AddLoadBalancerRequest request, StreamObserver<LoadBalancerId> responseObserver) {
        loadBalancerService.addLoadBalancer(request.getJobId(), request.getLoadBalancerId().getId()).subscribe(
                () -> {
                    responseObserver.onNext(request.getLoadBalancerId());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void removeLoadBalancer(RemoveLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        loadBalancerService.removeLoadBalancer(request.getJobId(), request.getLoadBalancerId().getId()).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }
}
