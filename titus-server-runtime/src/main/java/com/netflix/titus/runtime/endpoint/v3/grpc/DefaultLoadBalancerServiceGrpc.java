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

package com.netflix.titus.runtime.endpoint.v3.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.service.LoadBalancerService;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.logPageNumberUsage;

@Singleton
public class DefaultLoadBalancerServiceGrpc extends LoadBalancerServiceGrpc.LoadBalancerServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerServiceGrpc.class);
    private final LoadBalancerService loadBalancerService;
    private final SystemLogService systemLog;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultLoadBalancerServiceGrpc(LoadBalancerService loadBalancerService,
                                          SystemLogService systemLog,
                                          CallMetadataResolver callMetadataResolver) {
        this.loadBalancerService = loadBalancerService;
        this.systemLog = systemLog;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void getJobLoadBalancers(JobId request, StreamObserver<GetJobLoadBalancersResult> responseObserver) {
        logger.debug("Received get load balancer request {}", request);
        Subscription subscription = loadBalancerService.getLoadBalancers(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getAllLoadBalancers(com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest request,
                                    StreamObserver<GetAllLoadBalancersResult> responseObserver) {
        logger.debug("Received get all load balancer request {}", request);
        logPageNumberUsage(systemLog, callMetadataResolver, getClass().getSimpleName(), "getAllLoadBalancers", request.getPage());
        Subscription subscription = loadBalancerService.getAllLoadBalancers(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void addLoadBalancer(AddLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received add load balancer request {}", request);
        Subscription subscription = loadBalancerService.addLoadBalancer(request).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void removeLoadBalancer(RemoveLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received remove load balancer request {}", request);
        Subscription subscription = loadBalancerService.removeLoadBalancer(request).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                e -> safeOnError(logger, e, responseObserver)
        );
        attachCancellingCallback(responseObserver, subscription);
    }
}
