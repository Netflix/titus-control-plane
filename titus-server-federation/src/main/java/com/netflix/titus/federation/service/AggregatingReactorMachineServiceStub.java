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
import com.netflix.titus.grpc.protogen.v4.Id;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineQueryResult;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc.MachineServiceStub;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import com.netflix.titus.runtime.connector.machine.ReactorMachineServiceStub;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;
import rx.Observable;

import static com.netflix.titus.federation.service.PageAggregationUtil.combinePagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class AggregatingReactorMachineServiceStub implements ReactorMachineServiceStub {

    private final CallMetadataResolver callMetadataResolver;
    private final GrpcConfiguration grpcConfiguration;
    private final AggregatingCellClient aggregatingClient;

    @Inject
    public AggregatingReactorMachineServiceStub(CallMetadataResolver callMetadataResolver,
                                                GrpcConfiguration grpcConfiguration,
                                                AggregatingCellClient aggregatingClient) {
        this.callMetadataResolver = callMetadataResolver;
        this.grpcConfiguration = grpcConfiguration;
        this.aggregatingClient = aggregatingClient;
    }

    @Override
    public Mono<Machine> getMachine(Id request) {
        Observable<CellResponse<MachineServiceStub, Machine>> action = aggregatingClient
                .callExpectingErrors(MachineServiceGrpc::newStub, getMachineInCells(request))
                .reduce(ResponseMerger.singleValue())
                .flatMap(response -> response.getResult()
                        .map(v -> Observable.just(CellResponse.ofValue(response)))
                        .onErrorGet(Observable::error)
                );
        return ReactorExt.toFlux(action).last().map(CellResponse::getResult);
    }

    @Override
    public Mono<MachineQueryResult> getMachines(QueryRequest request) {
        Observable<MachineQueryResult> action = aggregatingClient
                .call(MachineServiceGrpc::newStub, getMachinesInCells(request))
                .map(CellResponse::getResult)
                .reduce((first, second) ->
                        MachineQueryResult.newBuilder()
                                .setPagination(combinePagination(first.getPagination(), second.getPagination()))
                                .addAllItems(first.getItemsList())
                                .addAllItems(second.getItemsList())
                                .build()
                );
        return ReactorExt.toFlux(action).last();
    }

    private ClientCall<Machine> getMachineInCells(Id request) {
        return (client, streamObserver) -> wrap(client).getMachine(request, streamObserver);
    }

    private ClientCall<MachineQueryResult> getMachinesInCells(QueryRequest request) {
        return (client, streamObserver) -> wrap(client).getMachines(request, streamObserver);
    }

    private MachineServiceStub wrap(MachineServiceStub client) {
        return GrpcUtil.createWrappedStubWithResolver(client, callMetadataResolver, grpcConfiguration.getRequestTimeoutMs());
    }

    private interface ClientCall<T> extends BiConsumer<MachineServiceStub, StreamObserver<T>> {
        // generics sanity
    }
}
