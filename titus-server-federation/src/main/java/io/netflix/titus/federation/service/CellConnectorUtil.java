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

package io.netflix.titus.federation.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.grpc.GrpcUtil;
import rx.Observable;

final class CellConnectorUtil {
    private CellConnectorUtil() {
    }

    static <T extends AbstractStub> Map<Cell, T> stubs(CellConnector connector, Function<ManagedChannel, T> stubFactory) {
        return connector.getChannels()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> stubFactory.apply(entry.getValue())
                ));
    }

    static <T extends AbstractStub> Optional<T> toStub(Cell cell, CellConnector connector, Function<ManagedChannel, T> stubFactory) {
        Optional<ManagedChannel> optionalChannel = connector.getChannelForCell(cell);
        return optionalChannel.map(stubFactory);
    }

    static <STUB extends AbstractStub<STUB>, RespT> List<Observable<CellResponse<STUB, RespT>>> callToAllCells(
            CellConnector connector,
            Function<ManagedChannel, STUB> stubFactory,
            BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        return callToAllCells(connector, stubFactory, false, fnCall);
    }

    static <STUB extends AbstractStub<STUB>, RespT> List<Observable<CellResponse<STUB, RespT>>> callToAllCells(
            CellConnector connector,
            Function<ManagedChannel, STUB> stubFactory,
            boolean swallowErrors,
            BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        Map<Cell, STUB> clients = stubs(connector, stubFactory);

        return clients.entrySet().stream().map(entry -> {
            final Cell cell = entry.getKey();
            STUB client = entry.getValue();
            Observable<RespT> request = GrpcUtil.createRequestObservable(emitter -> {
                StreamObserver<RespT> streamObserver = GrpcUtil.createSimpleClientResponseObserver(emitter);
                fnCall.accept(client, streamObserver);
            });
            Observable<CellResponse<STUB, RespT>> enhanced = request.map(result -> new CellResponse<>(cell, client, result));
            if (swallowErrors) {
                enhanced = enhanced.onErrorResumeNext(ignored -> Observable.empty());
            }
            return enhanced;
        }).collect(Collectors.toList());
    }

    static <STUB extends AbstractStub<STUB>, RespT> Observable<RespT> callToCell(
            Cell cell,
            CellConnector connector,
            Function<ManagedChannel, STUB> stubFactory,
            BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        Optional<ManagedChannel> channel = connector.getChannelForCell(cell);
        if (!channel.isPresent()) {
            return Observable.error(TitusServiceException.invalidArgument("Invalid Cell " + cell));
        }
        STUB targetClient = stubFactory.apply(channel.get());
        return GrpcUtil.createRequestObservable(emitter -> {
            StreamObserver<RespT> streamObserver = GrpcUtil.createSimpleClientResponseObserver(emitter);
            fnCall.accept(targetClient, streamObserver);
        });
    }

}

