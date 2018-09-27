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

package com.netflix.titus.testkit.embedded.cell.gateway;

import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.validator.PassJobValidator;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.gateway.startup.TitusGatewayModule;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.master.TitusMaster;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.testkit.util.NetworkExt;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;

/**
 * Run embedded version of TitusGateway.
 */
public class EmbeddedTitusGateway {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedTitusGateway.class);

    private static final CallMetadata CALL_METADATA = CallMetadata.newBuilder().withCallerId("gatewayClient").build();

    private final String masterGrpcHost;
    private final int masterGrpcPort;
    private final int masterHttpPort;

    private final int httpPort;
    private final int grpcPort;
    private final boolean enableREST;
    private final JobStore store;
    private final Properties properties;

    private final DefaultSettableConfig config;
    private final EntityValidator<JobDescriptor> validator;

    private LifecycleInjector injector;

    private ManagedChannel grpcChannel;

    public EmbeddedTitusGateway(Builder builder) {
        this.masterGrpcHost = getOrDefault(builder.masterGrpcHost, "localhost");
        this.masterGrpcPort = builder.masterGrpcPort;
        this.masterHttpPort = builder.masterHttpPort;
        this.httpPort = builder.httpPort;
        this.grpcPort = builder.grpcPort;
        this.enableREST = builder.enableREST;
        this.store = builder.store;
        this.properties = builder.properties;

        this.config = new DefaultSettableConfig();
        this.config.setProperties(properties);

        this.validator = builder.validator;

        String resourceDir = TitusMaster.class.getClassLoader().getResource("static").toExternalForm();
        Properties props = new Properties();
        props.put("titusMaster.v2Enabled", Boolean.toString(builder.v2Enabled));
        props.put("titus.masterClient.masterIp", masterGrpcHost);
        props.put("titus.masterClient.masterGrpcPort", masterGrpcPort);
        props.put("titus.masterClient.masterHttpPort", masterHttpPort);
        props.put("titusGateway.endpoint.grpc.port", grpcPort);
        props.put("governator.jetty.embedded.port", httpPort);
        props.put("governator.jetty.embedded.webAppResourceBase", resourceDir);
        props.put("titusMaster.job.configuration.defaultSecurityGroups", "sg-12345,sg-34567");
        props.put("titusMaster.job.configuration.defaultIamRole", "iam-12345");
        props.put("titusGateway.endpoint.grpc.loadbalancer.enabled", "true");
        config.setProperties(props);
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public EmbeddedTitusGateway boot() {
        logger.info("Starting Titus Gateway");

        injector = InjectorBuilder.fromModules(
                newJettyModule(),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindApplicationConfigurationOverride().toInstance(config);
                    }
                },
                Modules.override(new TitusGatewayModule(enableREST)).with(new AbstractModule() {
                    @Override
                    protected void configure() {
                        if (store != null) {
                            bind(JobStore.class).toInstance(store);
                        }

                        bind(EntityValidator.class).toInstance(validator);
                    }
                })
        ).createInjector();
        return this;
    }

    private Module newJettyModule() {
        return enableREST ? new Archaius2JettyModule() : Modules.EMPTY_MODULE;
    }

    public EmbeddedTitusGateway shutdown() {
        if (injector != null) {
            injector.close();
        }
        return this;
    }

    public HealthGrpc.HealthStub getHealthClient() {
        HealthGrpc.HealthStub client = HealthGrpc.newStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public JobManagementServiceGrpc.JobManagementServiceStub getV3GrpcClient() {
        JobManagementServiceGrpc.JobManagementServiceStub client = JobManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public JobManagementServiceGrpc.JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        JobManagementServiceGrpc.JobManagementServiceBlockingStub client = JobManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient() {
        AgentManagementServiceGrpc.AgentManagementServiceStub client = AgentManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient() {
        AgentManagementServiceGrpc.AgentManagementServiceBlockingStub client = AgentManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient() {
        AutoScalingServiceGrpc.AutoScalingServiceStub client = AutoScalingServiceGrpc.newStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        LoadBalancerServiceGrpc.LoadBalancerServiceStub client = LoadBalancerServiceGrpc.newStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public <I> I getInstance(Class<I> instanceType) {
        return injector.getInstance(instanceType);
    }

    private ManagedChannel getOrCreateGrpcChannel() {
        if (grpcChannel == null) {
            this.grpcChannel = NettyChannelBuilder.forAddress("localhost", grpcPort)
                    .usePlaintext(true)
                    .maxHeaderListSize(65536)
                    .build();
        }
        return grpcChannel;
    }

    private <STUB extends AbstractStub<STUB>> STUB attachCallHeaders(STUB client) {
        Metadata metadata = new Metadata();
        metadata.put(V3HeaderInterceptor.CALLER_ID_KEY, "embeddedGatewayClient");
        metadata.put(V3HeaderInterceptor.CALL_REASON_KEY, "test call");
        metadata.put(V3HeaderInterceptor.DEBUG_KEY, "true");
        return client.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    public Builder toBuilder() {
        return aDefaultTitusGateway()
                .withStore(store)
                .withMasterEndpoint(masterGrpcHost, masterGrpcPort, masterHttpPort)
                .withHttpPort(httpPort)
                .withGrpcPort(grpcPort)
                .withProperties(properties);
    }

    public static Builder aDefaultTitusGateway() {
        return new Builder()
                .withProperty("titusGateway.endpoint.grpc.shutdownTimeoutMs", "0");
    }

    public static class Builder {

        private String masterGrpcHost;
        private int masterGrpcPort;
        private int masterHttpPort;
        private int grpcPort;
        private int httpPort;
        private boolean enableREST = true;
        private JobStore store;
        private Properties properties = new Properties();
        private EntityValidator<JobDescriptor> validator = new PassJobValidator();

        // Enable V2 engine by default
        private boolean v2Enabled = true;

        public Builder withV2Engine(boolean v2Enabled) {
            this.v2Enabled = v2Enabled;
            return this;
        }

        public Builder withMasterEndpoint(String host, int grpcPort, int httpPort) {
            this.masterGrpcHost = host;
            this.masterGrpcPort = grpcPort;
            this.masterHttpPort = httpPort;
            return this;
        }

        public Builder withGrpcPort(int grpcPort) {
            this.grpcPort = grpcPort;
            return this;
        }

        public Builder withHttpPort(int httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Builder withEnableREST(boolean enableREST) {
            this.enableREST = enableREST;
            return this;
        }

        public Builder withStore(JobStore store) {
            this.store = store;
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

        public Builder withJobValidator(EntityValidator<JobDescriptor> validator) {
            this.validator = validator;
            return this;
        }

        public EmbeddedTitusGateway build() {
            httpPort = httpPort == 0 ? NetworkExt.findUnusedPort() : httpPort;
            grpcPort = grpcPort == 0 ? NetworkExt.findUnusedPort() : grpcPort;

            return new EmbeddedTitusGateway(this);
        }
    }
}
