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

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;


@Singleton
public class AggregatingJobManagementServiceHelper {

    private final AggregatingCellClient aggregatingCellClient;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public AggregatingJobManagementServiceHelper(AggregatingCellClient aggregatingCellClient,
                                                 GrpcConfiguration grpcConfiguration) {
        this.aggregatingCellClient = aggregatingCellClient;
        this.grpcConfiguration = grpcConfiguration;

    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub, CallMetadata callMetadata) {
        return createWrappedStub(stub, callMetadata, grpcConfiguration.getRequestTimeoutMs());
    }

    public Observable<CellResponse<JobManagementServiceStub, Job>> findJobInAllCells(String jobId, CallMetadata callMetadata) {
        return aggregatingCellClient.callExpectingErrors(JobManagementServiceGrpc::newStub, findJobInCell(jobId, callMetadata))
                .reduce(ResponseMerger.singleValue())
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
    }

    public Mono<CellResponse<JobManagementServiceStub, Job>> findJobInAllCellsReact(String jobId, CallMetadata callMetadata) {
        return ReactorExt.toMono(findJobInAllCells(jobId, callMetadata).toSingle());
    }

    public ClientCall<Job> findJobInCell(String jobId, CallMetadata callMetadata) {
        JobId id = JobId.newBuilder().setId(jobId).build();
        return (client, streamObserver) -> wrap(client, callMetadata).findJob(id, streamObserver);
    }

    public interface ClientCall<T> extends BiConsumer<JobManagementServiceStub, StreamObserver<T>> {
        // generics sanity
    }
}
