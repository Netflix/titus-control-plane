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

import java.util.function.BiConsumer;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.service.AutoScalingService;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.federation.service.CellConnectorUtil.callToCell;
import static com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceStub;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class AggregatingAutoScalingService implements AutoScalingService {
    private final CellConnector connector;
    private final AggregatingCellClient aggregatingClient;
    private final GrpcConfiguration grpcConfiguration;
    private final AggregatingJobManagementServiceHelper jobManagementServiceHelper;

    @Inject
    public AggregatingAutoScalingService(CellConnector connector,
                                         GrpcConfiguration configuration,
                                         AggregatingJobManagementServiceHelper jobManagementServiceHelper,
                                         AggregatingCellClient aggregatingClient) {
        this.connector = connector;
        grpcConfiguration = configuration;
        this.jobManagementServiceHelper = jobManagementServiceHelper;
        this.aggregatingClient = aggregatingClient;
    }

    @Override
    public Observable<GetPolicyResult> getJobScalingPolicies(JobId jobId, CallMetadata callMetadata) {
        return aggregatingClient.callExpectingErrors(AutoScalingServiceGrpc::newStub, getJobScalingPoliciesInCell(jobId, callMetadata))
                .filter(response -> response.getResult().hasError() || response.getResult().getValue().getItemsCount() > 0)
                .reduce(ResponseMerger.emptyResponseMarker(), ResponseMerger.singleValue())
                .filter(ResponseMerger::isNotEmptyResponseMarker)
                .flatMap(response -> response.getResult()
                        .map(Observable::just)
                        .onErrorGet(Observable::error)
                )
                .switchIfEmpty(Observable.just(GetPolicyResult.newBuilder().build()));
    }

    private ClientCall<GetPolicyResult> getJobScalingPoliciesInCell(JobId jobId, CallMetadata callMetadata) {
        return (client, responseObserver) -> wrap(client, callMetadata).getJobScalingPolicies(jobId, responseObserver);
    }

    @Override
    public Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request, CallMetadata callMetadata) {
        JobId jobId = JobId.newBuilder().setId(request.getJobId()).build();
        return jobManagementServiceHelper.findJobInAllCells(jobId.getId(), callMetadata)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.setAutoScalingPolicy(request, responseObserver),
                        callMetadata)
                );
    }

    @Override
    public Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request, CallMetadata callMetadata) {
        // each Cell returns an Status.INTERNAL error today when the id is not found
        // TODO: make the error condition more explicit and return NOT_ERROR
        return getScalingPolicyInAllCells(request, callMetadata).map(CellResponse::getResult);
    }

    /**
     * @return can be an empty Observable. Callers must handle it appropriately, usually with
     * {@link Observable#switchIfEmpty(Observable)}.
     */
    private Observable<CellResponse<AutoScalingServiceStub, GetPolicyResult>> getScalingPolicyInAllCells(ScalingPolicyID request,
                                                                                                         CallMetadata callMetadata) {
        return aggregatingClient.callExpectingErrors(AutoScalingServiceGrpc::newStub, getScalingPolicyInCell(request, callMetadata))
                .filter(response -> response.getResult().hasError() || response.getResult().getValue().getItemsCount() > 0)
                // Observable#reduce does not support empty Observables, so we inject a marker and take it out after
                // merging results. The marker will be passed through only when the Observable is empty
                .reduce(ResponseMerger.emptyResponseMarker(), ResponseMerger.singleValue())
                .filter(ResponseMerger::isNotEmptyResponseMarker)
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
    }

    private ClientCall<GetPolicyResult> getScalingPolicyInCell(ScalingPolicyID request, CallMetadata callMetadata) {
        return (client, responseObserver) -> wrap(client, callMetadata).getScalingPolicy(request, responseObserver);
    }

    @Override
    public Observable<GetPolicyResult> getAllScalingPolicies(CallMetadata callMetadata) {
        return aggregatingClient.call(AutoScalingServiceGrpc::newStub, getAllScalingPoliciesInCell(callMetadata))
                .map(CellResponse::getResult)
                .reduce((acc, next) -> GetPolicyResult.newBuilder()
                        .addAllItems(acc.getItemsList())
                        .addAllItems(next.getItemsList())
                        .build()
                );
    }

    private ClientCall<GetPolicyResult> getAllScalingPoliciesInCell(CallMetadata callMetadata) {
        return (client, responseObserver) ->
                wrap(client, callMetadata).getAllScalingPolicies(Empty.getDefaultInstance(), responseObserver);
    }

    @Override
    public Completable deleteAutoScalingPolicy(DeletePolicyRequest request, CallMetadata callMetadata) {
        ScalingPolicyID policyId = request.getId();
        Observable<Empty> observable = getScalingPolicyInAllCells(policyId, callMetadata)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.deleteAutoScalingPolicy(request, responseObserver),
                        callMetadata)
                );
        return observable.toCompletable();
    }

    @Override
    public Completable updateAutoScalingPolicy(UpdatePolicyRequest request, CallMetadata callMetadata) {
        ScalingPolicyID policyId = request.getPolicyId();
        Observable<Empty> observable = getScalingPolicyInAllCells(policyId, callMetadata)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.updateAutoScalingPolicy(request, responseObserver),
                        callMetadata)
                );
        return observable.toCompletable();
    }

    private <T> Observable<T> singleCellCall(Cell cell, ClientCall<T> clientCall, CallMetadata callMetadata) {
        return callToCell(cell, connector, AutoScalingServiceGrpc::newStub,
                (client, streamObserver) -> clientCall.accept(wrap(client, callMetadata), streamObserver));
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub, CallMetadata callMetadata) {
        return createWrappedStub(stub, callMetadata, grpcConfiguration.getRequestTimeoutMs());
    }

    private interface ClientCall<T> extends BiConsumer<AutoScalingServiceStub, StreamObserver<T>> {
        // generics sanity
    }
}
