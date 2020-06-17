/*
 * Copyright 2019 Netflix, Inc.
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

import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class DefaultAggregatingSchedulerService implements AggregatingSchedulerService {

    private final GrpcConfiguration grpcConfiguration;
    private final AggregatingCellClient aggregatingClient;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultAggregatingSchedulerService(GrpcConfiguration grpcConfiguration,
                                              AggregatingCellClient aggregatingClient,
                                              CallMetadataResolver callMetadataResolver) {
        this.grpcConfiguration = grpcConfiguration;
        this.aggregatingClient = aggregatingClient;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public Mono<SchedulingResultEvent> findLastSchedulingResult(String taskId) {
        return findSchedulingResultInAllCells(taskId).map(CellResponse::getResult);
    }

    private Mono<CellResponse<SchedulerServiceGrpc.SchedulerServiceStub, SchedulingResultEvent>> findSchedulingResultInAllCells(String taskId) {
        Observable<CellResponse<SchedulerServiceGrpc.SchedulerServiceStub, SchedulingResultEvent>> action = aggregatingClient.callExpectingErrors(SchedulerServiceGrpc::newStub, findSchedulingResultInCell(taskId))
                .reduce(ResponseMerger.singleValue())
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
        return ReactorExt.toFlux(action).last();
    }

    private ClientCall<SchedulingResultEvent> findSchedulingResultInCell(String taskId) {
        SchedulingResultRequest request = SchedulingResultRequest.newBuilder().setTaskId(taskId).build();
        return (client, streamObserver) -> wrap(client).getSchedulingResult(request, streamObserver);
    }

    private SchedulerServiceGrpc.SchedulerServiceStub wrap(SchedulerServiceGrpc.SchedulerServiceStub client) {
        return GrpcUtil.createWrappedStubWithResolver(client, callMetadataResolver, grpcConfiguration.getRequestTimeoutMs());
    }

    private interface ClientCall<T> extends BiConsumer<SchedulerServiceGrpc.SchedulerServiceStub, StreamObserver<T>> {
        // generics sanity
    }
}
