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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.tuple.Either;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.federation.service.CellConnectorUtil.stubs;

@Singleton
class AggregatingCellClient {
    private final CellConnector connector;

    @Inject
    public AggregatingCellClient(CellConnector connector) {
        this.connector = connector;
    }

    /**
     * Call services on all Cells and collect results. Results from each {@link Cell} are emitted individually on the
     * returned {@link Observable}. The first error encountered will be propagated (if any).
     */
    <STUB extends AbstractStub<STUB>, RespT> Observable<CellResponse<STUB, RespT>> call(
            Function<ManagedChannel, STUB> stubFactory,
            BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        Map<Cell, STUB> clients = stubs(connector, stubFactory);
        List<Observable<CellResponse<STUB, RespT>>> results = clients.entrySet().stream().map(entry -> {
            Cell cell = entry.getKey();
            STUB client = entry.getValue();
            Observable<RespT> request = createRequestObservable(emitter -> {
                StreamObserver<RespT> streamObserver = createSimpleClientResponseObserver(emitter);
                fnCall.accept(client, streamObserver);
            });
            return request.map(result -> new CellResponse<>(cell, client, result));
        }).collect(Collectors.toList());

        return Observable.merge(results);
    }

    /**
     * Call services on all Cells and collect results, which can be {@link Either Either<RespT, Throwable>}. Results
     * from each {@link Cell} are emitted individually on the returned {@link Observable}.
     */
    <STUB extends AbstractStub<STUB>, RespT>
    Observable<CellResponse<STUB, Either<RespT, Throwable>>> callExpectingErrors(
            Function<ManagedChannel, STUB> stubFactory,
            BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        Map<Cell, STUB> clients = stubs(connector, stubFactory);
        List<Observable<CellResponse<STUB, Either<RespT, Throwable>>>> results = clients.entrySet().stream().map(entry -> {
            Cell cell = entry.getKey();
            STUB client = entry.getValue();
            Observable<RespT> request = callSingleCell(client, fnCall);
            return request.map(result ->
                    new CellResponse<>(cell, client, Either.<RespT, Throwable>ofValue(result))
            ).onErrorResumeNext(error -> Observable.just(
                    new CellResponse<>(cell, client, Either.ofError(error)))
            );
        }).collect(Collectors.toList());

        return Observable.merge(results);
    }

    private <STUB extends AbstractStub<STUB>, RespT>
    Observable<RespT> callSingleCell(STUB client, BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        return createRequestObservable(emitter -> {
            StreamObserver<RespT> streamObserver = createSimpleClientResponseObserver(emitter);
            fnCall.accept(client, streamObserver);
        });
    }
}
