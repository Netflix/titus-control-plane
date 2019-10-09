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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.titus.federation.endpoint.grpc.TitusFederationGrpcServer;
import com.netflix.titus.federation.startup.TitusFederationModule;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.master.TitusMaster;
import com.netflix.titus.runtime.endpoint.common.rest.EmbeddedJettyModule;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run embedded version of TitusFederation.
 */
public class EmbeddedTitusFederation {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedTitusFederation.class);

    private final Map<String, CellInfo> cells;

    private int httpPort;
    private final int grpcPort;
    private final Properties properties;

    private final DefaultSettableConfig config;

    private LifecycleInjector injector;

    private ManagedChannel grpcChannel;

    private final EmbeddedTitusOperations titusOperations;

    public EmbeddedTitusFederation(Builder builder) {
        this.cells = builder.cells;
        this.httpPort = builder.httpPort;
        this.grpcPort = builder.grpcPort;
        this.properties = builder.properties;

        this.config = new DefaultSettableConfig();
        this.config.setProperties(properties);

        String resourceDir = TitusMaster.class.getClassLoader().getResource("static").toExternalForm();
        Properties props = new Properties();
        if (!properties.containsKey("titus.federation.stack")) {
            props.put("titus.federation.stack", "embedded");
        }
        props.put("titus.federation.endpoint.grpcPort", grpcPort);
        props.put("titus.federation.routingRules", buildRoutingRules());
        props.put("governator.jetty.embedded.webAppResourceBase", resourceDir);
        config.setProperties(props);

        this.titusOperations = new EmbeddedFederationTitusOperations(this);
    }

    public int getGrpcPort() {
        Preconditions.checkNotNull(injector);
        return injector.getInstance(TitusFederationGrpcServer.class).getGrpcPort();
    }

    private String buildCellString() {
        StringBuilder sb = new StringBuilder();
        cells.forEach((cellId, cellInfo) ->
                sb.append(';').append(cellId).append("=localhost:").append(cellInfo.getCell().getGateway().getGrpcPort())
        );
        return sb.substring(1);
    }

    private String buildRoutingRules() {
        StringBuilder sb = new StringBuilder();
        cells.forEach((cellId, cellInfo) ->
                sb.append(';').append(cellId).append('=').append(cellInfo.getRoutingRules())
        );
        return sb.substring(1);
    }

    public EmbeddedTitusFederation boot() {
        logger.info("Starting Titus Federation");

        injector = InjectorBuilder.fromModules(
                new EmbeddedJettyModule(httpPort),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        // We can set some properties only after the gateway is started.ł
                        config.setProperty("titus.federation.cells", buildCellString());

                        bindApplicationConfigurationOverride().toInstance(config);
                    }
                },
                new TitusFederationModule()
        ).createInjector();
        return this;
    }

    public EmbeddedTitusFederation shutdown() {
        if (injector != null) {
            injector.close();
        }
        return this;
    }

    public EmbeddedTitusCell getCell(String cellId) {
        return cells.get(cellId).getCell();
    }

    public List<EmbeddedTitusCell> getCells() {
        return new ArrayList<>(cells.values().stream().map(CellInfo::getCell).collect(Collectors.toList()));
    }

    public EmbeddedTitusOperations getTitusOperations() {
        return titusOperations;
    }

    public HealthGrpc.HealthStub getHealthGrpcClient() {
        HealthGrpc.HealthStub client = HealthGrpc.newStub(getOrCreateGrpcChannel());
        return attachCallHeaders(client);
    }

    public SchedulerServiceGrpc.SchedulerServiceBlockingStub getV3BlockingSchedulerClient() {
        SchedulerServiceGrpc.SchedulerServiceBlockingStub client = SchedulerServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
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

    public EvictionServiceGrpc.EvictionServiceBlockingStub getBlockingGrpcEvictionClient() {
        return EvictionServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
    }

    private ManagedChannel getOrCreateGrpcChannel() {
        if (grpcChannel == null) {
            this.grpcChannel = ManagedChannelBuilder.forAddress("localhost", getGrpcPort())
                    .usePlaintext(true)
                    .build();
        }
        return grpcChannel;
    }

    private <STUB extends AbstractStub<STUB>> STUB attachCallHeaders(STUB client) {
        Metadata metadata = new Metadata();
        metadata.put(V3HeaderInterceptor.CALLER_ID_KEY, "embeddedFederationClient");
        metadata.put(V3HeaderInterceptor.CALL_REASON_KEY, "test call");
        metadata.put(V3HeaderInterceptor.DEBUG_KEY, "true");
        return client.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    public Builder toBuilder() {
        Builder builder = aDefaultTitusFederation()
                .withHttpPort(httpPort)
                .withGrpcPort(grpcPort)
                .withProperties(properties);

        cells.forEach((cellId, cellInfo) -> builder.withCell(cellInfo.getRoutingRules(), cellInfo.getCell()));

        return builder;
    }

    public static Builder aDefaultTitusFederation() {
        return new Builder()
                .withProperty("titus.federation.endpoint.grpcServerShutdownTimeoutMs", "0");
    }

    public static class Builder {

        private final Map<String, CellInfo> cells = new HashMap<>();
        private int grpcPort;
        private int httpPort;
        private Properties properties = new Properties();

        public Builder withCell(String routingRules, EmbeddedTitusCell cell) {
            String cellId = cell.getMaster().getCellName();
            cells.put(cellId, new CellInfo(cellId, routingRules, cell));
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
            return new EmbeddedTitusFederation(this);
        }
    }

    private static class CellInfo {

        private final String cellId;
        private final EmbeddedTitusCell cell;
        private final String routingRules;

        private CellInfo(String cellId, String routingRules, EmbeddedTitusCell cell) {
            this.cellId = cellId;
            this.cell = cell;
            this.routingRules = routingRules;
        }

        private String getCellId() {
            return cellId;
        }

        private String getRoutingRules() {
            return routingRules;
        }

        private EmbeddedTitusCell getCell() {
            return cell;
        }
    }
}
