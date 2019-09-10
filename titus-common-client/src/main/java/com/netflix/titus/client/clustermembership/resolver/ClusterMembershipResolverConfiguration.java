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

package com.netflix.titus.client.clustermembership.resolver;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.clusterMembership.client.resolver")
public interface ClusterMembershipResolverConfiguration {

    /**
     * Frequency at which GRPC stream connection is disconnected to prevent staleness.
     */
    @DefaultValue("60000")
    long getSingleMemberReconnectIntervalMs();

    @DefaultValue("100")
    long getSingleMemberInitialRetryIntervalMs();

    @DefaultValue("1000")
    long getSingleMemberMaxRetryIntervalMs();

    @DefaultValue("30000")
    long getHealthDisconnectThresholdMs();

    /**
     * Interval at which data from the underlying resolvers is collected and evaluated to produce a new cluster
     * membership state.
     */
    @DefaultValue("100")
    long getMultiMemberRefreshIntervalMs();
}
