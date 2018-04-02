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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.federation.model.Cell;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultCellConnector implements CellConnector {
    private static Logger logger = LoggerFactory.getLogger(DefaultCellConnector.class);

    private static final long SHUTDOWN_TIMEOUT_MS = 5_000;
    private final Map<Cell, ManagedChannel> channels;

    @Inject
    public DefaultCellConnector(CellInfoResolver cellInfoResolver) {
        List<Cell> cells = cellInfoResolver.resolve();

        if (cells != null && !cells.isEmpty()) {
            channels = cellInfoResolver.resolve()
                    .stream()
                    .collect(Collectors.toMap(cell -> cell,
                            cell -> NettyChannelBuilder.forTarget(cell.getAddress())
                                    .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                                    .build()));
        } else {
            channels = Collections.emptyMap();
        }
    }

    @Override
    public Map<Cell, ManagedChannel> getChannels() {
        return channels;
    }

    @Override
    public Optional<ManagedChannel> getChannelForCell(Cell cell) {
        return Optional.ofNullable(channels.get(cell));
    }

    @PreDestroy
    public void shutdown() {
        channels.values()
                .forEach(channel -> {
                    logger.info("shutting down gRPC channels");
                    channel.shutdown();
                    try {
                        channel.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (!channel.isTerminated()) {
                            channel.shutdownNow();
                        }
                    }
                });
    }
}
