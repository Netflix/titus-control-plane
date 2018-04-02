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

package com.netflix.titus.master.loadbalancer.endpoint.grpc;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.stub.StreamObserver;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.master.loadbalancer.endpoint.grpc.GrpcModelConverters.toGetAllLoadBalancersResult;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;

@Singleton
public class DefaultLoadBalancerServiceGrpc extends LoadBalancerServiceGrpc.LoadBalancerServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerServiceGrpc.class);
    private final LoadBalancerService loadBalancerService;

    @Inject
    public DefaultLoadBalancerServiceGrpc(LoadBalancerService loadBalancerService) {
        this.loadBalancerService = loadBalancerService;
    }

    @Override
    public void getJobLoadBalancers(JobId request, StreamObserver<GetJobLoadBalancersResult> responseObserver) {
        logger.debug("Received get load balancer request {}", request);
        GetJobLoadBalancersResult.Builder resultBuilder = GetJobLoadBalancersResult.newBuilder()
                .setJobId(request.getId());
        loadBalancerService.getJobLoadBalancers(request.getId()).subscribe(
                loadBalancerId -> resultBuilder.addLoadBalancers(LoadBalancerId.newBuilder().setId(loadBalancerId).build()),
                e -> safeOnError(logger, e, responseObserver),
                () -> {
                    responseObserver.onNext(resultBuilder.build());
                    responseObserver.onCompleted();
                }
        );
    }

    @Override
    public void getAllLoadBalancers(GetAllLoadBalancersRequest request,
                                    StreamObserver<GetAllLoadBalancersResult> responseObserver) {
        logger.debug("Received get all load balancer request {}", request);
        Page grpcPage = request.getPage();
        if (!checkPageIsValid(grpcPage, responseObserver)) {
            return;
        }

        try {
            Pair<List<JobLoadBalancer>, Pagination> pageResult = loadBalancerService.getAllLoadBalancers(toPage(grpcPage));
            responseObserver.onNext(toGetAllLoadBalancersResult(pageResult.getLeft(), pageResult.getRight()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            safeOnError(logger, e, responseObserver);
        }
    }

    @Override
    public void addLoadBalancer(AddLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received add load balancer request {}", request);
        loadBalancerService.addLoadBalancer(request.getJobId(), request.getLoadBalancerId().getId()).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        );
    }

    @Override
    public void removeLoadBalancer(RemoveLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received remove load balancer request {}", request);
        loadBalancerService.removeLoadBalancer(request.getJobId(), request.getLoadBalancerId().getId()).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        );
    }
}
