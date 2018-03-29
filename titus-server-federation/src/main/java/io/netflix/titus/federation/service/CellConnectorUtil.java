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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import io.netflix.titus.api.federation.model.Cell;

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
        return optionalChannel.flatMap(managedChannel -> Optional.of(stubFactory.apply(optionalChannel.get())));
    }
}

