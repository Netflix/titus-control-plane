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

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
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
import rx.Emitter;
import rx.Observable;
import rx.functions.Action1;

import static com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.METHOD_ADD_LOAD_BALANCER;
import static com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.METHOD_GET_ALL_LOAD_BALANCERS;
import static com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.METHOD_GET_JOB_LOAD_BALANCERS;
import static com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.METHOD_REMOVE_LOAD_BALANCER;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final GrpcClientConfiguration configuration;
    private LoadBalancerServiceGrpc.LoadBalancerServiceStub client;
    private final SessionContext sessionContext;
    private final LoadBalancerResourceValidator validator;

    @Inject
    public DefaultLoadBalancerService(GrpcClientConfiguration configuration,
                                      LoadBalancerResourceValidator validator,
                                      LoadBalancerServiceGrpc.LoadBalancerServiceStub client,
                                      SessionContext sessionContext) {
        this.configuration = configuration;
        this.client = client;
        this.sessionContext = sessionContext;
        this.validator = validator;
    }

    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId) {
        return toObservable(emitter -> {
            StreamObserver<GetJobLoadBalancersResult> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_GET_JOB_LOAD_BALANCERS, jobId, simpleStreamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request) {
        return toObservable(emitter -> {
            StreamObserver<GetAllLoadBalancersResult> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            ClientCall clientCall = call(METHOD_GET_ALL_LOAD_BALANCERS, request, simpleStreamObserver);
            GrpcUtil.attachCancellingCallback(emitter, clientCall);
        });
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest addLoadBalancerRequest) {
        return validator.validateLoadBalancer(addLoadBalancerRequest.getLoadBalancerId().getId())
                .onErrorResumeNext(e -> Completable.error(TitusServiceException.invalidArgument(e.getMessage())))
                .andThen(toCompletable(emitter -> {
                    StreamObserver<Empty> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
                    ClientCall clientCall = call(METHOD_ADD_LOAD_BALANCER, addLoadBalancerRequest, simpleStreamObserver);
                    GrpcUtil.attachCancellingCallback(emitter, clientCall);
                }));
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest) {
        return toCompletable(emptyEmitter -> {
            StreamObserver<Empty> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emptyEmitter);
            ClientCall clientCall = call(METHOD_REMOVE_LOAD_BALANCER, removeLoadBalancerRequest, simpleStreamObserver);
            GrpcUtil.attachCancellingCallback(emptyEmitter, clientCall);
        });
    }

    private Completable toCompletable(Action1<Emitter<Empty>> emitter) {
        return toObservable(emitter).toCompletable();
    }

    private <ReqT, RespT> ClientCall call(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request, StreamObserver<RespT> responseObserver) {
        return GrpcUtil.call(sessionContext, client, methodDescriptor, request, responseObserver);
    }

    private <T> Observable<T> toObservable(Action1<Emitter<T>> emitter) {
        return Observable.create(
                emitter,
                Emitter.BackpressureMode.NONE
        ).timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }
}
