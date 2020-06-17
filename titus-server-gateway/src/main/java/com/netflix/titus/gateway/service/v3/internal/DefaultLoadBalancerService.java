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

package com.netflix.titus.gateway.service.v3.internal;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerResourceValidator;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceStub;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.service.LoadBalancerService;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {

    private final GrpcClientConfiguration configuration;
    private final LoadBalancerServiceStub client;
    private final LoadBalancerResourceValidator validator;

    @Inject
    public DefaultLoadBalancerService(GrpcClientConfiguration configuration,
                                      LoadBalancerResourceValidator validator,
                                      LoadBalancerServiceStub client) {
        this.configuration = configuration;
        this.client = client;
        this.validator = validator;
    }

    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<GetJobLoadBalancersResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).getJobLoadBalancers(jobId, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<GetAllLoadBalancersResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).getAllLoadBalancers(request, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest addLoadBalancerRequest, CallMetadata callMetadata) {
        return validator.validateLoadBalancer(addLoadBalancerRequest.getLoadBalancerId().getId())
                .onErrorResumeNext(e -> Completable.error(TitusServiceException.invalidArgument(e.getMessage())))
                .andThen(createRequestCompletable(emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
                    createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).addLoadBalancer(addLoadBalancerRequest, streamObserver);
                }, configuration.getRequestTimeout()));
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeout()).removeLoadBalancer(removeLoadBalancerRequest, streamObserver);
        }, configuration.getRequestTimeout());
    }
}
