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

package com.netflix.titus.testkit.embedded.federation;

import java.util.Properties;

import com.google.inject.AbstractModule;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;
import com.netflix.titus.common.grpc.AnonymousSessionContext;
import com.netflix.titus.common.grpc.SessionContext;
import com.netflix.titus.common.grpc.V3HeaderInterceptor;
import com.netflix.titus.federation.startup.TitusFederationModule;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.master.TitusMaster;
import com.netflix.titus.testkit.util.NetworkExt;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;

/**
 * Run embedded version of TitusFederation.
 */
public class EmbeddedTitusFederation {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedTitusFederation.class);

    private final String gatewayGrpcHost;
    private final int gatewayGrpcPort;

    private final int httpPort;
    private final int grpcPort;
    private final Properties properties;

    private final DefaultSettableConfig config;

    private LifecycleInjector injector;

    private ManagedChannel grpcChannel;

    public EmbeddedTitusFederation(Builder builder) {
        this.gatewayGrpcHost = getOrDefault(builder.gatewayGrpcHost, "localhost");
        this.gatewayGrpcPort = builder.gatewayGrpcPort;
        this.httpPort = builder.httpPort;
        this.grpcPort = builder.grpcPort;
        this.properties = builder.properties;

        this.config = new DefaultSettableConfig();
        this.config.setProperties(properties);

        String resourceDir = TitusMaster.class.getClassLoader().getResource("static").toExternalForm();
        Properties props = new Properties();
        props.put("titus.federation.endpoint.grpcPort", grpcPort);
        props.put("titus.federation.cells", String.format("cell1=%s:%s", gatewayGrpcHost, gatewayGrpcPort));
        props.put("titus.federation.routingRules", "cell1=.*");
        props.put("governator.jetty.embedded.port", httpPort);
        props.put("governator.jetty.embedded.webAppResourceBase", resourceDir);
        config.setProperties(props);
    }

    public EmbeddedTitusFederation boot() {
        logger.info("Starting Titus Federation");

        injector = InjectorBuilder.fromModules(
                new Archaius2JettyModule(),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindApplicationConfigurationOverride().toInstance(config);
                    }
                },
                new TitusFederationModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(SessionContext.class).to(AnonymousSessionContext.class);
                    }
                }
        ).createInjector();
        return this;
    }

    public EmbeddedTitusFederation shutdown() {
        if (injector != null) {
            injector.close();
        }
        return this;
    }

    public JobManagementServiceGrpc.JobManagementServiceStub getV3GrpcClient() {
        JobManagementServiceGrpc.JobManagementServiceStub client = JobManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    public JobManagementServiceGrpc.JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        JobManagementServiceGrpc.JobManagementServiceBlockingStub client = JobManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    public AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient() {
        AgentManagementServiceGrpc.AgentManagementServiceStub client = AgentManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    public AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient() {
        AgentManagementServiceGrpc.AgentManagementServiceBlockingStub client = AgentManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    public AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient() {
        AutoScalingServiceGrpc.AutoScalingServiceStub client = AutoScalingServiceGrpc.newStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        LoadBalancerServiceGrpc.LoadBalancerServiceStub client = LoadBalancerServiceGrpc.newStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    private ManagedChannel getOrCreateGrpcChannel() {
        if (grpcChannel == null) {
            this.grpcChannel = ManagedChannelBuilder.forAddress("localhost", grpcPort)
                    .usePlaintext(true)
                    .build();
        }
        return grpcChannel;
    }

    public Builder toBuilder() {
        return aDefaultTitusFederation()
                .witGatewayEndpoint(gatewayGrpcHost, gatewayGrpcPort)
                .withHttpPort(httpPort)
                .withGrpcPort(grpcPort)
                .withProperties(properties);
    }

    public static Builder aDefaultTitusFederation() {
        return new Builder()
                .withProperty("titusGateway.endpoint.grpc.shutdownTimeoutMs", "0");
    }

    public static class Builder {

        private String gatewayGrpcHost;
        private int gatewayGrpcPort;
        private int grpcPort;
        private int httpPort;
        private Properties properties = new Properties();

        public Builder witGatewayEndpoint(String host, int grpcPort) {
            this.gatewayGrpcHost = host;
            this.gatewayGrpcPort = grpcPort;
            return this;
        }

        public Builder withHttpPort(int httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Builder withGrpcPort(int grpcPort) {
            this.grpcPort = grpcPort;
            return this;
        }

        public Builder withProperty(String name, String value) {
            properties.put(name, value);
            return this;
        }

        public Builder withProperties(Properties properties) {
            this.properties.putAll(properties);
            return this;
        }

        public EmbeddedTitusFederation build() {
            httpPort = httpPort == 0 ? NetworkExt.findUnusedPort() : httpPort;
            grpcPort = grpcPort == 0 ? NetworkExt.findUnusedPort() : grpcPort;

            return new EmbeddedTitusFederation(this);
        }
    }
}
