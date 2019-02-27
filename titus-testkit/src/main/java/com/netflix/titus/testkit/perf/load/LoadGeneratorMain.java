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

package com.netflix.titus.testkit.perf.load;

import java.util.List;
import javax.inject.Named;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.connector.agent.AgentManagerConnectorComponent;
import com.netflix.titus.runtime.connector.eviction.EvictionConnectorComponent;
import com.netflix.titus.runtime.connector.jobmanager.JobManagerConnectorComponent;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.testkit.embedded.cloud.connector.remote.SimulatedRemoteInstanceCloudConnector;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class LoadGeneratorMain {

    private static final String GATEWAY_CHANNEL = "gatewayChannel";

    /**
     * Can be TitusGateway or TitusFederation.
     */
    private static final String API_ACCESS_CHANNEL = "apiAccessChannel";

    @Bean
    public TitusRuntime getTitusRuntime() {
        return TitusRuntimes.internal();
    }

    @Bean
    public CallMetadataResolver getCallMetadataResolver() {
        return AnonymousCallMetadataResolver.getInstance();
    }

    @Bean
    @Named(GATEWAY_CHANNEL)
    public Channel getGatewayChannel(ApplicationArguments arguments) {
        List<String> gateway = arguments.getOptionValues("gateway");

        String gatewayAddress = CollectionsExt.isNullOrEmpty(gateway) ? "localhost:8091" : gateway.get(0);

        return NettyChannelBuilder
                .forTarget(gatewayAddress)
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    @Bean
    @Named(API_ACCESS_CHANNEL)
    public Channel getApiAccessChannel(@Named(GATEWAY_CHANNEL) Channel gatewayChannel,
                                       ApplicationArguments arguments) {
        List<String> federation = arguments.getOptionValues("federation");

        if (CollectionsExt.isNullOrEmpty(federation)) {
            return gatewayChannel;
        }

        return NettyChannelBuilder
                .forTarget(federation.get(0))
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    @Bean
    @Named(SimulatedRemoteInstanceCloudConnector.SIMULATED_CLOUD)
    public Channel getSimulatedCloudChannel(ApplicationArguments arguments) {
        List<String> cloud = arguments.getOptionValues("cloud");

        String cloudAddress = CollectionsExt.isNullOrEmpty(cloud) ? "localhost:8093" : cloud.get(0);

        return NettyChannelBuilder
                .forTarget(cloudAddress)
                .usePlaintext(true)
                .maxHeaderListSize(65536)
                .build();
    }

    @Bean
    @Named(AgentManagerConnectorComponent.AGENT_CHANNEL)
    public Channel getAgentManagerChannel(@Named(GATEWAY_CHANNEL) Channel channel) {
        return channel;
    }

    @Bean
    @Named(JobManagerConnectorComponent.JOB_MANAGER_CHANNEL)
    public Channel getJobManagerChannel(@Named(API_ACCESS_CHANNEL) Channel channel) {
        return channel;
    }

    @Bean
    @Named(EvictionConnectorComponent.EVICTION_CHANNEL)
    public Channel getEvictionChannel(@Named(GATEWAY_CHANNEL) Channel channel) {
        return channel;
    }

    public static void main(String[] args) {
        SpringApplication.run(LoadGeneratorMain.class, args);
    }
}
