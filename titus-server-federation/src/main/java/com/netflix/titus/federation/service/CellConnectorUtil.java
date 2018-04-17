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

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
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

