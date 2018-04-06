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

import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import static com.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;


@Singleton
public class AggregatingJobManagementServiceHelper {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingJobManagementServiceHelper.class);
    private AggregatingCellClient aggregatingCellClient;
    private final GrpcConfiguration grpcConfiguration;
    private final SessionContext sessionContext;

    @Inject
    public AggregatingJobManagementServiceHelper(AggregatingCellClient aggregatingCellClient,
                                                 GrpcConfiguration grpcConfiguration,
                                                 SessionContext sessionContext) {
        this.aggregatingCellClient = aggregatingCellClient;
        this.grpcConfiguration = grpcConfiguration;
        this.sessionContext = sessionContext;

    }

    private <STUB extends AbstractStub<STUB>> STUB wrap(STUB stub) {
        return createWrappedStub(stub, sessionContext, grpcConfiguration.getRequestTimeoutMs());
    }

    public Observable<CellResponse<JobManagementServiceStub, Job>> findJobInAllCells(String jobId) {
        return aggregatingCellClient.callExpectingErrors(JobManagementServiceGrpc::newStub, findJobInCell(jobId))
                .reduce(ResponseMerger.singleValue())
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
    }

    public ClientCall<Job> findJobInCell(String jobId) {
        JobId id = JobId.newBuilder().setId(jobId).build();
        return (client, streamObserver) -> wrap(client).findJob(id, streamObserver);
    }

    public interface ClientCall<T> extends BiConsumer<JobManagementServiceStub, StreamObserver<T>> {
        // generics sanity
    }

}
