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
package io.netflix.titus.federation.service;

import java.util.function.BiConsumer;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceStub;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.federation.startup.GrpcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static io.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;
import static io.netflix.titus.federation.service.CellConnectorUtil.callToAllCells;

@Singleton
public class AggregatingLoadbalancerService implements LoadBalancerService {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingLoadbalancerService.class);
    private CellConnector connector;
    private final SessionContext sessionContext;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingLoadbalancerService(CellConnector connector,
                                          SessionContext sessionContext,
                                          GrpcConfiguration grpcConfiguration) {
        this.connector = connector;
        this.sessionContext = sessionContext;
        this.grpcConfiguration = grpcConfiguration;
    }

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request) {
        return Observable.error(notImplemented("addLoadBalancer"));
    }

    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId) {
        BiConsumer<LoadBalancerServiceStub, StreamObserver<GetJobLoadBalancersResult>> getLoadBalancersForJob =
                (client, responseObserver) -> wrap(client).getJobLoadBalancers(jobId, responseObserver);

        return Observable.mergeDelayError(
                callToAllCells(connector, LoadBalancerServiceGrpc::newStub, true, getLoadBalancersForJob)
        ).compose(combineLBResults());
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest request) {
        return Completable.error(notImplemented("addLoadBalancer"));
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest) {
        return Completable.error(notImplemented("removeLoadBalancer"));
    }


    private static StatusException notImplemented(String operation) {
        return Status.UNIMPLEMENTED.withDescription(operation + " is not implemented").asException();
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub) {
        return createWrappedStub(stub, sessionContext, grpcConfiguration.getRequestTimeoutMs());
    }

    private Observable.Transformer<CellResponse<LoadBalancerServiceStub, GetJobLoadBalancersResult>, GetJobLoadBalancersResult> combineLBResults() {
        return lbResults -> lbResults.reduce(GetJobLoadBalancersResult.newBuilder().build(),
                (acc, next) -> GetJobLoadBalancersResult.newBuilder()
                        .addAllLoadBalancers(acc.getLoadBalancersList())
                        .addAllLoadBalancers(next.getResult().getLoadBalancersList())
                        .build());
    }


}
