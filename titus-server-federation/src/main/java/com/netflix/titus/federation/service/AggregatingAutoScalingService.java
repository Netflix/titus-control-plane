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
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;
import static com.netflix.titus.federation.service.CellConnectorUtil.callToAllCells;
import static com.netflix.titus.federation.service.CellConnectorUtil.callToCell;
import static com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceStub;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;

@Singleton
public class AggregatingAutoScalingService implements AutoScalingService {
    private CellConnector connector;
    private SessionContext sessionContext;
    private GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingAutoScalingService(CellConnector connector,
                                         SessionContext sessionContext,
                                         GrpcConfiguration configuration) {
        this.connector = connector;
        this.sessionContext = sessionContext;
        grpcConfiguration = configuration;
    }

    @Override
    public Observable<GetPolicyResult> getJobScalingPolicies(JobId jobId) {
        BiConsumer<AutoScalingServiceStub, StreamObserver<GetPolicyResult>> getJobScalingPolicies =
                (client, responseObserver) -> wrap(client).getJobScalingPolicies(jobId, responseObserver);

        return Observable.mergeDelayError(
                callToAllCells(connector, AutoScalingServiceGrpc::newStub, true, getJobScalingPolicies)
        ).compose(combinePolicyResults());
    }

    @Override
    public Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request) {
        JobId jobId = JobId.newBuilder().setId(request.getJobId()).build();
        BiConsumer<JobManagementServiceStub, StreamObserver<Job>> findJob =
                (client, responseObserver) -> wrap(client).findJob(jobId, responseObserver);
        BiConsumer<AutoScalingServiceStub, StreamObserver<ScalingPolicyID>> setPolicy =
                (client, responseObserver) -> wrap(client).setAutoScalingPolicy(request, responseObserver);

        return Observable.mergeDelayError(
                callToAllCells(connector, JobManagementServiceGrpc::newStub, true, findJob)
        ).flatMap(response ->
                callToCell(response.getCell(), connector, AutoScalingServiceGrpc::newStub, setPolicy)
        );
    }

    @Override
    public Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request) {
        BiConsumer<AutoScalingServiceStub, StreamObserver<GetPolicyResult>> getScalingPolicy =
                (client, responseObserver) -> wrap(client).getScalingPolicy(request, responseObserver);

        return Observable.mergeDelayError(
                callToAllCells(connector, AutoScalingServiceGrpc::newStub, true, getScalingPolicy)
        ).compose(combinePolicyResults());
    }

    @Override
    public Observable<GetPolicyResult> getAllScalingPolicies() {
        BiConsumer<AutoScalingServiceStub, StreamObserver<GetPolicyResult>> getAllPolicies =
                (client, responseObserver) -> wrap(client).getAllScalingPolicies(Empty.getDefaultInstance(), responseObserver);

        return Observable.mergeDelayError(
                callToAllCells(connector, AutoScalingServiceGrpc::newStub, getAllPolicies)
        ).compose(combinePolicyResults());
    }

    @Override
    public Completable deleteAutoScalingPolicy(DeletePolicyRequest request) {
        ScalingPolicyID policyId = request.getId();
        BiConsumer<AutoScalingServiceStub, StreamObserver<GetPolicyResult>> getScalingPolicy =
                (client, responseObserver) -> wrap(client).getScalingPolicy(policyId, responseObserver);
        BiConsumer<AutoScalingServiceStub, StreamObserver<Empty>> deleteAutoScalingPolicy =
                (client, responseObserver) -> wrap(client).deleteAutoScalingPolicy(request, responseObserver);

        Observable<CellResponse<AutoScalingServiceStub, GetPolicyResult>> cellResponses = Observable.mergeDelayError(
                callToAllCells(connector, AutoScalingServiceGrpc::newStub, true, getScalingPolicy)
        );
        return cellResponses
                .filter(response -> response.getResult().getItemsCount() > 0)
                .flatMap(t -> callToCell(t.getCell(), connector, AutoScalingServiceGrpc::newStub, deleteAutoScalingPolicy))
                .toCompletable();
    }

    @Override
    public Completable updateAutoScalingPolicy(UpdatePolicyRequest request) {
        ScalingPolicyID policyId = request.getPolicyId();
        BiConsumer<AutoScalingServiceStub, StreamObserver<GetPolicyResult>> getScalingPolicy =
                (client, responseObserver) -> wrap(client).getScalingPolicy(policyId, responseObserver);

        Observable<CellResponse<AutoScalingServiceStub, GetPolicyResult>> cellResponses = Observable.mergeDelayError(
                callToAllCells(connector, AutoScalingServiceGrpc::newStub, true, getScalingPolicy)
        );

        Observable<Empty> update = cellResponses.filter(response -> response.getResult().getItemsCount() > 0)
                .flatMap(response -> createRequestObservable(emitter -> {
                    StreamObserver<Empty> responseObserver = createSimpleClientResponseObserver(emitter);
                    wrap(response.getClient()).updateAutoScalingPolicy(request, responseObserver);
                }));
        return update.toCompletable();
    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub) {
        return createWrappedStub(stub, sessionContext, grpcConfiguration.getRequestTimeoutMs());
    }

    private Observable.Transformer<CellResponse<AutoScalingServiceStub, GetPolicyResult>, GetPolicyResult> combinePolicyResults() {
        return policyResults -> policyResults.reduce(GetPolicyResult.newBuilder().build(),
                (acc, next) -> GetPolicyResult.newBuilder()
                        .addAllItems(acc.getItemsList())
                        .addAllItems(next.getResult().getItemsList())
                        .build());
    }

}
