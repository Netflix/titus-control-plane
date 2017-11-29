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
import com.netflix.titus.grpc.protogen.GetLoadBalancerResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerResourceValidator;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.gateway.service.v3.GrpcClientConfiguration;
import io.netflix.titus.gateway.service.v3.LoadBalancerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.functions.Action1;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final GrpcClientConfiguration configuration;
    private LoadBalancerServiceGrpc.LoadBalancerServiceStub client;
    private final LoadBalancerResourceValidator validator;

    @Inject
    public DefaultLoadBalancerService(GrpcClientConfiguration configuration,
                                      LoadBalancerResourceValidator validator,
                                      LoadBalancerServiceGrpc.LoadBalancerServiceStub client) {
        this.configuration = configuration;
        this.client = client;
        this.validator = validator;
    }

    @Override
    public Observable<GetLoadBalancerResult> getLoadBalancers(JobId jobId) {
        return toObservable(emitter -> {
            StreamObserver<GetLoadBalancerResult> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.getJobLoadBalancers(jobId, simpleStreamObserver);
        });
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest addLoadBalancerRequest) {
        try {
            validator.validateLoadBalancer(addLoadBalancerRequest.getLoadBalancerId().getId());
        } catch (Exception e) {
            return Completable.error(TitusServiceException.invalidArgument(e.getMessage()));
        }

        return toCompletable(emitter -> {
            StreamObserver<Empty> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emitter);
            client.addLoadBalancer(addLoadBalancerRequest, simpleStreamObserver);
        });
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest) {
        return toCompletable(emptyEmitter -> {
            StreamObserver<Empty> simpleStreamObserver = GrpcUtil.createSimpleStreamObserver(emptyEmitter);
            client.removeLoadBalancer(removeLoadBalancerRequest, simpleStreamObserver);
        });
    }

    private Completable toCompletable(Action1<Emitter<Empty>> emitter) {
        return toObservable(emitter).toCompletable();
    }

    private <T> Observable<T> toObservable(Action1<Emitter<T>> emitter) {
        return Observable.create(
                emitter,
                Emitter.BackpressureMode.NONE
        ).timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }
}
