/*
 * Copyright 2017 Netflix, Inc.
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
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.federation.startup.TitusFederationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultCellConnector implements CellConnector {
    private static Logger logger = LoggerFactory.getLogger(DefaultCellConnector.class);
    private final List<Cell> cells;
    private final Map<Cell, ManagedChannel> channels;

    @Inject
    public DefaultCellConnector(TitusFederationConfiguration appConfig) {
        cells = CellInfoUtil.extractCellsFromCellSpecification(appConfig.getCells());
        channels = cells.stream()
                .collect(Collectors.toMap(cell -> cell,
                        cell -> NettyChannelBuilder.forTarget(cell.getAddress())
                                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                                .build()));
    }

    @Override
    public Map<Cell, ManagedChannel> getChannels() {
        return channels;
    }

    @Override
    public Optional<ManagedChannel> getChannelForCell(Cell cell) {
        if (channels.containsKey(cell)) {
            return Optional.of(channels.get(cell));
        }
        return Optional.empty();
    }
}
