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

package com.netflix.titus.supplementary.relocation;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.titusmaster.LeaderNameResolverFactory;
import com.netflix.titus.runtime.connector.titusmaster.LeaderResolver;
import com.netflix.titus.runtime.connector.titusmaster.TitusMasterClientConfiguration;
import com.netflix.titus.supplementary.relocation.connector.TitusMasterConnectorComponent;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class RelocationMain {

    @Bean(name = TitusMasterConnectorComponent.TITUS_MASTER_CHANNEL)
    public Channel getManagedChannel(TitusMasterClientConfiguration configuration, LeaderResolver leaderResolver, TitusRuntime titusRuntime) {
        return NettyChannelBuilder
                .forTarget("leader://titusmaster")
                .nameResolverFactory(new LeaderNameResolverFactory(leaderResolver, configuration.getMasterGrpcPort(), titusRuntime))
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(RelocationMain.class, args);
    }
}
