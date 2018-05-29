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

package com.netflix.titus.master.health.service;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.netflix.titus.common.util.guice.ActivationLifecycle;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.ServerStatus;
import com.netflix.titus.grpc.protogen.ServiceActivation;
import com.netflix.titus.master.cluster.LeaderActivator;
import com.netflix.titus.master.config.CellInfoResolver;

import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.NOT_SERVING;
import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.SERVING;

@Singleton
public class DefaultHealthService implements HealthService {
    private final CellInfoResolver cellInfoResolver;
    private final ActivationLifecycle activationLifecycle;
    private final LeaderActivator leaderActivator;

    @Inject
    public DefaultHealthService(CellInfoResolver cellInfoResolver,
                                ActivationLifecycle activationLifecycle,
                                LeaderActivator leaderActivator) {
        this.cellInfoResolver = cellInfoResolver;
        this.activationLifecycle = activationLifecycle;
        this.leaderActivator = leaderActivator;
    }

    public ServerStatus getServerStatus() {
        RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
        Duration uptime = Durations.fromMillis(rb.getUptime());

        if (!leaderActivator.isLeader()) {
            return ServerStatus.newBuilder()
                    .setStatus(NOT_SERVING)
                    .setLeader(false)
                    .setActive(false)
                    .setUptime(uptime)
                    .build();
        }

        boolean active = leaderActivator.isActivated();

        List<ServiceActivation> serviceActivations = activationLifecycle.getServiceActionTimesMs().stream()
                .sorted(Comparator.comparing(Pair::getRight))
                .map(pair -> ServiceActivation.newBuilder()
                        .setName(pair.getLeft())
                        .setActivationTime(Durations.fromMillis(pair.getRight()))
                        .build())
                .collect(Collectors.toList());

        ServerStatus.Builder details = ServerStatus.newBuilder()
                .setStatus(SERVING)
                .setCell(cellInfoResolver.getCellName())
                .setLeader(true)
                .setActive(active)
                .setUptime(uptime)
                .setElectionTimestamp(Timestamps.fromMillis(leaderActivator.getElectionTimestamp()))
                .setActivationTimestamp(Timestamps.fromMillis(leaderActivator.getActivationEndTimestamp()))
                .addAllServiceActivationTimes(serviceActivations);

        if (active) {
            details.setActivationTime(Durations.fromMillis(leaderActivator.getActivationTime()));
        }

        return details.build();
    }
}
