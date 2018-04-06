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

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc.LoadBalancerServiceStub;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.loadbalancer.GrpcModelConverters;
import com.netflix.titus.runtime.loadbalancer.LoadBalancerCursors;
import com.netflix.titus.runtime.service.LoadBalancerService;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.federation.service.CellConnectorUtil.callToCell;
import static com.netflix.titus.federation.service.PageAggregationUtil.combinePagination;
import static com.netflix.titus.federation.service.PageAggregationUtil.takeCombinedPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.emptyGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPagination;

@Singleton
public class AggregatingLoadbalancerService implements LoadBalancerService {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingLoadbalancerService.class);
    private final AggregatingCellClient aggregatingClient;
    private final AggregatingJobManagementServiceHelper jobManagementServiceHelper;
    private CellConnector connector;
    private final SessionContext sessionContext;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingLoadbalancerService(CellConnector connector,
                                          SessionContext sessionContext,
                                          GrpcConfiguration grpcConfiguration,
                                          AggregatingCellClient aggregatingClient,
                                          AggregatingJobManagementServiceHelper jobManagementServiceHelper) {
        this.connector = connector;
        this.sessionContext = sessionContext;
        this.grpcConfiguration = grpcConfiguration;
        this.aggregatingClient = aggregatingClient;
        this.jobManagementServiceHelper = jobManagementServiceHelper;
    }

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request) {
        if (request.getPage().getPageSize() <= 0) {
            return Observable.just(GetAllLoadBalancersResult.newBuilder()
                    .setPagination(emptyGrpcPagination(request.getPage()))
                    .build());
        }
        if (StringExt.isNotEmpty(request.getPage().getCursor()) || request.getPage().getPageNumber() == 0) {
            return findLoadBalancersWithCursorPagination(request);
        }
        return Observable.error(TitusServiceException.invalidArgument("pageNumbers are not supported, please use cursors"));
    }


    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId) {
        BiConsumer<LoadBalancerServiceStub, StreamObserver<GetJobLoadBalancersResult>> getLoadBalancersForJob =
                (client, responseObserver) -> wrap(client).getJobLoadBalancers(jobId, responseObserver);

        return aggregatingClient.callExpectingErrors(LoadBalancerServiceGrpc::newStub, getLoadBalancersForJob)
                .filter(response -> response.getResult().hasError() || response.getResult().getValue().getLoadBalancersCount() > 0)
                .reduce(ResponseMerger.emptyResponseMarker(), ResponseMerger.singleValue())
                .filter(ResponseMerger::isNotEmptyResponseMarker)
                .flatMap(response -> response.getResult().map(Observable::just).onErrorGet(Observable::error));
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest request) {
        JobId jobId = JobId.newBuilder().setId(request.getJobId()).build();
        final Observable<Empty> responseObservable = jobManagementServiceHelper.findJobInAllCells(jobId.getId())
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.addLoadBalancer(request, responseObserver)));
        return responseObservable.toCompletable();
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest) {
        JobId jobId = JobId.newBuilder().setId(removeLoadBalancerRequest.getJobId()).build();
        final Observable<Empty> responseObservable = jobManagementServiceHelper.findJobInAllCells(jobId.getId())
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.removeLoadBalancer(removeLoadBalancerRequest, responseObserver)));
        return responseObservable.toCompletable();
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub) {
        return createWrappedStub(stub, sessionContext, grpcConfiguration.getRequestTimeoutMs());
    }

    private <T> Observable<T> singleCellCall(Cell cell, ClientCall<T> clientCall) {
        return callToCell(cell, connector, LoadBalancerServiceGrpc::newStub,
                (client, streamObserver) -> clientCall.accept(wrap(client), streamObserver));
    }

    private interface ClientCall<T> extends BiConsumer<LoadBalancerServiceStub, StreamObserver<T>> {
        // generics sanity
    }

    private ClientCall<GetAllLoadBalancersResult> getAllLoadBalancersInCell(GetAllLoadBalancersRequest request) {
        return (client, streamObserver) -> wrap(client).getAllLoadBalancers(request, streamObserver);
    }

    private Observable<GetAllLoadBalancersResult> findLoadBalancersWithCursorPagination(GetAllLoadBalancersRequest request) {
        return aggregatingClient.call(LoadBalancerServiceGrpc::newStub, getAllLoadBalancersInCell(request))
                .map(CellResponse::getResult)
                .reduce(this::combineGetAllJobLoadBalancersResult)
                .map(combinedResults -> {
                    Pair<List<JobLoadBalancer>, Pagination> combinedPage = takeCombinedPage(
                            request.getPage(),
                            toListOfJobLoadBalancer(combinedResults),
                            combinedResults.getPagination(),
                            LoadBalancerCursors.loadBalancerComparator(),
                            LoadBalancerCursors::newCursorFrom);

                    return GrpcModelConverters.toGetAllLoadBalancersResult(combinedPage.getLeft(), toPagination(combinedPage.getRight()));
                });
    }

    private GetAllLoadBalancersResult combineGetAllJobLoadBalancersResult(GetAllLoadBalancersResult one,
                                                                          GetAllLoadBalancersResult other) {
        Pagination pagination = combinePagination(one.getPagination(), other.getPagination());
        return GetAllLoadBalancersResult.newBuilder()
                .setPagination(pagination)
                .addAllJobLoadBalancers(one.getJobLoadBalancersList())
                .addAllJobLoadBalancers(other.getJobLoadBalancersList())
                .build();
    }


    private List<JobLoadBalancer> toListOfJobLoadBalancer(GetAllLoadBalancersResult result) {
        return result.getJobLoadBalancersList().stream()
                .flatMap(jobLoadBalancersResult -> {
                    final String jobId = jobLoadBalancersResult.getJobId();
                    return jobLoadBalancersResult.getLoadBalancersList().stream()
                            .map(loadBalancerId -> new JobLoadBalancer(jobId, loadBalancerId.getId()));
                })
                .collect(Collectors.toList());
    }


}
