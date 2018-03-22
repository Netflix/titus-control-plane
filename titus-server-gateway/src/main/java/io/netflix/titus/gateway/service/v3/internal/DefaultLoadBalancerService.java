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

package io.netflix.titus.gateway.service.v3.internal;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceStub;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerResourceValidator;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import io.netflix.titus.gateway.service.v3.LoadBalancerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static io.netflix.titus.common.grpc.GrpcUtil.createRequestCompletable;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static io.netflix.titus.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static io.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final GrpcClientConfiguration configuration;
    private LoadBalancerServiceStub client;
    private final SessionContext sessionContext;
    private final LoadBalancerResourceValidator validator;

    @Inject
    public DefaultLoadBalancerService(GrpcClientConfiguration configuration,
                                      LoadBalancerResourceValidator validator,
                                      LoadBalancerServiceStub client,
                                      SessionContext sessionContext) {
        this.configuration = configuration;
        this.client = client;
        this.sessionContext = sessionContext;
        this.validator = validator;
    }

    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId) {
        return createRequestObservable(emitter -> {
            StreamObserver<GetJobLoadBalancersResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getJobLoadBalancers(jobId, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request) {
        return createRequestObservable(emitter -> {
            StreamObserver<GetAllLoadBalancersResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).getAllLoadBalancers(request, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest addLoadBalancerRequest) {
        return validator.validateLoadBalancer(addLoadBalancerRequest.getLoadBalancerId().getId())
                .onErrorResumeNext(e -> Completable.error(TitusServiceException.invalidArgument(e.getMessage())))
                .andThen(createRequestCompletable(emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
                    createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).addLoadBalancer(addLoadBalancerRequest, streamObserver);
                }, configuration.getRequestTimeout()));
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, sessionContext, configuration.getRequestTimeout()).removeLoadBalancer(removeLoadBalancerRequest, streamObserver);
        }, configuration.getRequestTimeout());
    }
}
