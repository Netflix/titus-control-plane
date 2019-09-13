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

package com.netflix.titus.runtime.clustermembership.connector;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.NetworkExt;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Schedulers;

@Configuration
public class ClusterMembershipInMemoryConnectorComponent {

    @Bean
    @ConditionalOnMissingBean
    public ClusterMembershipRevision<ClusterMember> getInitialLocalMemberRevision(TitusRuntime titusRuntime) {
        return newInitialLocalMemberRevision(7004, titusRuntime);
    }

    @Bean
    public ClusterMembershipInMemoryConnector getClusterMembershipConnector(ClusterMembershipRevision<ClusterMember> initialLocalMemberRevision,
                                                                            TitusRuntime titusRuntime) {
        return new ClusterMembershipInMemoryConnector(initialLocalMemberRevision, titusRuntime, Schedulers.parallel());
    }

    public static ClusterMembershipRevision<ClusterMember> newInitialLocalMemberRevision(int grpcPort, TitusRuntime titusRuntime) {
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(ClusterMember.newBuilder()
                        .withMemberId(NetworkExt.getHostName().orElse(String.format("%s@localhost", System.getenv("USER"))))
                        .withEnabled(true)
                        .withRegistered(false)
                        .withActive(false)
                        .withClusterMemberAddresses(getLocalAddresses(grpcPort))
                        .withLabels(Collections.emptyMap())
                        .build()
                )
                .withRevision(0)
                .withCode("started")
                .withMessage("Initial local membership record")
                .withTimestamp(titusRuntime.getClock().wallTime())
                .build();
    }

    private static List<ClusterMemberAddress> getLocalAddresses(int grpcPort) {
        return NetworkExt.getLocalIPs()
                .map(addresses -> addresses.stream()
                        .map(ipAddress -> newAddress(grpcPort, ipAddress))
                        .collect(Collectors.toList())
                )
                .orElse(Collections.singletonList(newAddress(grpcPort, "localhost")));
    }

    private static ClusterMemberAddress newAddress(int grpcPort, String ipAddress) {
        return ClusterMemberAddress.newBuilder()
                .withIpAddress(ipAddress)
                .withProtocol("grpc")
                .withPortNumber(grpcPort)
                .withDescription("GRPC endpoint")
                .build();
    }
}
