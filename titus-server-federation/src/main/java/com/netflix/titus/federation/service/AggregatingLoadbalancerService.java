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
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.service.TitusServiceException;
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
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.loadbalancer.GrpcModelConverters;
import com.netflix.titus.runtime.loadbalancer.LoadBalancerCursors;
import com.netflix.titus.runtime.service.LoadBalancerService;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.federation.service.CellConnectorUtil.callToCell;
import static com.netflix.titus.federation.service.PageAggregationUtil.combinePagination;
import static com.netflix.titus.federation.service.PageAggregationUtil.takeCombinedPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.emptyGrpcPagination;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toPagination;

@Singleton
public class AggregatingLoadbalancerService implements LoadBalancerService {

    private final AggregatingCellClient aggregatingClient;
    private final AggregatingJobManagementServiceHelper jobManagementServiceHelper;
    private final CellConnector connector;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingLoadbalancerService(CellConnector connector,
                                          CallMetadataResolver callMetadataResolver,
                                          GrpcConfiguration grpcConfiguration,
                                          AggregatingCellClient aggregatingClient,
                                          AggregatingJobManagementServiceHelper jobManagementServiceHelper) {
        this.connector = connector;
        this.grpcConfiguration = grpcConfiguration;
        this.aggregatingClient = aggregatingClient;
        this.jobManagementServiceHelper = jobManagementServiceHelper;
    }

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request, CallMetadata callMetadata) {
        if (request.getPage().getPageSize() <= 0) {
            return Observable.just(GetAllLoadBalancersResult.newBuilder()
                    .setPagination(emptyGrpcPagination(request.getPage()))
                    .build());
        }
        if (StringExt.isNotEmpty(request.getPage().getCursor()) || request.getPage().getPageNumber() == 0) {
            return findLoadBalancersWithCursorPagination(request, callMetadata);
        }
        return Observable.error(TitusServiceException.invalidArgument("pageNumbers are not supported, please use cursors"));
    }


    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId, CallMetadata callMetadata) {
        ClientCall<GetJobLoadBalancersResult> getLoadBalancersForJob =
                (client, responseObserver) -> wrap(client, callMetadata).getJobLoadBalancers(jobId, responseObserver);

        return aggregatingClient.callExpectingErrors(LoadBalancerServiceGrpc::newStub, getLoadBalancersForJob)
                .filter(response -> response.getResult().hasError() || response.getResult().getValue().getLoadBalancersCount() > 0)
                .reduce(ResponseMerger.emptyResponseMarker(), ResponseMerger.singleValue())
                .filter(ResponseMerger::isNotEmptyResponseMarker)
                .flatMap(response -> response.getResult().map(Observable::just).onErrorGet(Observable::error))
                .switchIfEmpty(Observable.just(GetJobLoadBalancersResult.newBuilder().setJobId(jobId.getId()).build()));
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest request, CallMetadata callMetadata) {
        JobId jobId = JobId.newBuilder().setId(request.getJobId()).build();
        final Observable<Empty> responseObservable = jobManagementServiceHelper.findJobInAllCells(jobId.getId(), callMetadata)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.addLoadBalancer(request, responseObserver),
                        callMetadata));
        return responseObservable.toCompletable();
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest, CallMetadata callMetadata) {
        JobId jobId = JobId.newBuilder().setId(removeLoadBalancerRequest.getJobId()).build();
        final Observable<Empty> responseObservable = jobManagementServiceHelper.findJobInAllCells(jobId.getId(), callMetadata)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.removeLoadBalancer(removeLoadBalancerRequest, responseObserver),
                        callMetadata));
        return responseObservable.toCompletable();
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub, CallMetadata callMetadata) {
        return createWrappedStub(stub, callMetadata, grpcConfiguration.getRequestTimeoutMs());
    }

    private <T> Observable<T> singleCellCall(Cell cell, ClientCall<T> clientCall, CallMetadata callMetadata) {
        return callToCell(cell, connector, LoadBalancerServiceGrpc::newStub,
                (client, streamObserver) -> clientCall.accept(wrap(client, callMetadata), streamObserver));
    }

    private interface ClientCall<T> extends BiConsumer<LoadBalancerServiceStub, StreamObserver<T>> {
        // generics sanity
    }

    private ClientCall<GetAllLoadBalancersResult> getAllLoadBalancersInCell(GetAllLoadBalancersRequest request, CallMetadata callMetadata) {
        return (client, streamObserver) -> wrap(client, callMetadata).getAllLoadBalancers(request, streamObserver);
    }

    private Observable<GetAllLoadBalancersResult> findLoadBalancersWithCursorPagination(GetAllLoadBalancersRequest request, CallMetadata callMetadata) {
        return aggregatingClient.call(LoadBalancerServiceGrpc::newStub, getAllLoadBalancersInCell(request, callMetadata))
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
