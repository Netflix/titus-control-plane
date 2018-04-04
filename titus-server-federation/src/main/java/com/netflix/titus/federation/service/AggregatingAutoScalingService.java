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
import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.service.AutoScalingService;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.federation.service.CellConnectorUtil.callToCell;
import static com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceStub;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;

@Singleton
public class AggregatingAutoScalingService implements AutoScalingService {
    private final CellConnector connector;
    private final AggregatingCellClient aggregatingClient;
    private final SessionContext sessionContext;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingAutoScalingService(CellConnector connector,
                                         SessionContext sessionContext,
                                         GrpcConfiguration configuration) {
        this.aggregatingClient = new AggregatingCellClient(connector);
        this.connector = connector;
        this.sessionContext = sessionContext;
        grpcConfiguration = configuration;
    }

    @Override
    public Observable<GetPolicyResult> getJobScalingPolicies(JobId jobId) {
        return aggregatingClient.callExpectingErrors(AutoScalingServiceGrpc::newStub, getJobScalingPoliciesInCell(jobId))
                .filter(response -> response.getResult().hasError() || response.getResult().getValue().getItemsCount() > 0)
                .reduce(ResponseMerger.emptyResponseMarker(), ResponseMerger.singleValue())
                .filter(ResponseMerger::isNotEmptyResponseMarker)
                .flatMap(response -> response.getResult()
                        .map(Observable::just)
                        .onErrorGet(Observable::error)
                )
                .switchIfEmpty(Observable.just(GetPolicyResult.newBuilder().build()));
    }

    private ClientCall<GetPolicyResult> getJobScalingPoliciesInCell(JobId jobId) {
        return (client, responseObserver) -> wrap(client).getJobScalingPolicies(jobId, responseObserver);
    }

    @Override
    public Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request) {
        JobId jobId = JobId.newBuilder().setId(request.getJobId()).build();
        return findJobInAllCells(jobId)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.setAutoScalingPolicy(request, responseObserver))
                );
    }

    private Observable<CellResponse<JobManagementServiceStub, Job>> findJobInAllCells(JobId jobId) {
        return aggregatingClient.callExpectingErrors(JobManagementServiceGrpc::newStub, findJobInCell(jobId))
                .reduce(ResponseMerger.singleValue())
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
    }

    private BiConsumer<JobManagementServiceStub, StreamObserver<Job>> findJobInCell(JobId jobId) {
        return (client, responseObserver) -> wrap(client).findJob(jobId, responseObserver);
    }

    @Override
    public Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request) {
        return getScalingPolicyInAllCells(request).map(CellResponse::getResult)
                .switchIfEmpty(Observable.just(GetPolicyResult.newBuilder().build()));
    }

    /**
     * @return can be an empty Observable. Callers must handle it appropriately, usually with
     * {@link Observable#switchIfEmpty(Observable)}.
     */
    private Observable<CellResponse<AutoScalingServiceStub, GetPolicyResult>> getScalingPolicyInAllCells(ScalingPolicyID request) {
        return aggregatingClient.callExpectingErrors(AutoScalingServiceGrpc::newStub, getScalingPolicyInCell(request))
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

    private ClientCall<GetPolicyResult> getScalingPolicyInCell(ScalingPolicyID request) {
        return (client, responseObserver) -> wrap(client).getScalingPolicy(request, responseObserver);
    }

    @Override
    public Observable<GetPolicyResult> getAllScalingPolicies() {
        return aggregatingClient.call(AutoScalingServiceGrpc::newStub, getAllScalingPoliciesInCell())
                .map(CellResponse::getResult)
                .reduce((acc, next) -> GetPolicyResult.newBuilder()
                        .addAllItems(acc.getItemsList())
                        .addAllItems(next.getItemsList())
                        .build()
                );
    }

    private ClientCall<GetPolicyResult> getAllScalingPoliciesInCell() {
        return (client, responseObserver) ->
                wrap(client).getAllScalingPolicies(Empty.getDefaultInstance(), responseObserver);
    }

    @Override
    public Completable deleteAutoScalingPolicy(DeletePolicyRequest request) {
        ScalingPolicyID policyId = request.getId();
        Observable<Empty> observable = getScalingPolicyInAllCells(policyId)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.deleteAutoScalingPolicy(request, responseObserver))
                );
        return observable.toCompletable();
    }

    @Override
    public Completable updateAutoScalingPolicy(UpdatePolicyRequest request) {
        ScalingPolicyID policyId = request.getPolicyId();
        Observable<Empty> observable = getScalingPolicyInAllCells(policyId)
                .flatMap(response -> singleCellCall(response.getCell(),
                        (client, responseObserver) -> client.updateAutoScalingPolicy(request, responseObserver))
                );
        return observable.toCompletable();
    }

    private <T> Observable<T> singleCellCall(Cell cell, ClientCall<T> clientCall) {
        return callToCell(cell, connector, AutoScalingServiceGrpc::newStub,
                (client, streamObserver) -> clientCall.accept(wrap(client), streamObserver));
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub) {
        return createWrappedStub(stub, sessionContext, grpcConfiguration.getRequestTimeoutMs());
    }

    private interface ClientCall<T> extends BiConsumer<AutoScalingServiceStub, StreamObserver<T>> {
        // generics sanity
    }
}
