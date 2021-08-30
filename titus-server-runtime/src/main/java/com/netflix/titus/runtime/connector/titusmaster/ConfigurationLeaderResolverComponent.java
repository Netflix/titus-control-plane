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

package com.netflix.titus.runtime.connector.titusmaster;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigurationLeaderResolverComponent {

    public static final String TITUS_MASTER_CHANNEL = "titusMasterChannel";

    @Bean
    public TitusMasterClientConfiguration getTitusMasterClientConfiguration(TitusRuntime titusRuntime) {
        return Archaius2Ext.newConfiguration(TitusMasterClientConfiguration.class, "titus.masterClient", titusRuntime.getMyEnvironment());
    }

    @Bean
    public LeaderResolver getLeaderResolver(TitusMasterClientConfiguration configuration) {
        return new ConfigurationLeaderResolver(configuration);
    }

    @Bean(name = TITUS_MASTER_CHANNEL)
    public Channel getManagedChannel(TitusMasterClientConfiguration configuration, LeaderResolver leaderResolver, TitusRuntime titusRuntime) {
        return NettyChannelBuilder
                .forTarget("leader://titusmaster")
                .nameResolverFactory(new LeaderNameResolverFactory(leaderResolver, configuration.getMasterGrpcPort(), titusRuntime))
                .usePlaintext()
                .maxInboundMetadataSize(65536)
                .build();
    }
}
