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

package com.netflix.titus.testkit.embedded.cell.master;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.util.Modules;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.jetty.JettyModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.store.AgentStore;
import com.netflix.titus.api.appscale.store.AppScalePolicyStore;
import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.api.audit.service.AuditLogService;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.connector.cloud.noop.NoOpInstanceCloudConnector;
import com.netflix.titus.api.connector.cloud.noop.NoOpLoadBalancerConnector;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import com.netflix.titus.api.loadbalancer.model.sanitizer.NoOpLoadBalancerJobValidator;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.api.supervisor.service.LeaderActivator;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import com.netflix.titus.api.supervisor.service.MasterMonitor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import com.netflix.titus.common.util.guice.ContainerEventBus;
import com.netflix.titus.ext.cassandra.testkit.store.EmbeddedCassandraStoreFactory;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc.HealthStub;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceBlockingStub;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc;
import com.netflix.titus.master.TitusMaster;
import com.netflix.titus.master.TitusMasterModule;
import com.netflix.titus.master.TitusRuntimeModule;
import com.netflix.titus.master.endpoint.grpc.TitusMasterGrpcServer;
import com.netflix.titus.master.eviction.service.quota.system.ArchaiusSystemDisruptionBudgetResolver;
import com.netflix.titus.master.eviction.service.quota.system.SystemDisruptionBudgetDescriptor;
import com.netflix.titus.master.supervisor.service.leader.LocalMasterMonitor;
import com.netflix.titus.runtime.endpoint.common.rest.EmbeddedJettyModule;
import com.netflix.titus.runtime.store.v3.memory.InMemoryJobStore;
import com.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import com.netflix.titus.runtime.store.v3.memory.InMemoryPolicyStore;
import com.netflix.titus.testkit.client.ReactorTitusMasterClient;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeModule;
import com.netflix.titus.testkit.grpc.TestKitGrpcClientErrorUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Run TitusMaster server with mocked external integrations (mesos, storage).
 */
public class EmbeddedTitusMaster {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedTitusMaster.class);

    public static final String CELL_NAME = "embeddedCell";

    private final Properties properties;
    private final DefaultSettableConfig config;
    private int apiPort;
    private int grpcPort;
    private final boolean enableREST;
    private final boolean enableDisruptionBudget;
    private final String cellName;
    private final MasterDescription masterDescription;

    private final JobStore jobStore;
    private final boolean cassandraJobStore;

    private final EmbeddedKubeCluster embeddedKubeCluster;

    private LifecycleInjector injector;
    private final List<AuditLogEvent> auditLogs = new CopyOnWriteArrayList<>();

    private ManagedChannel grpcChannel;

    private EmbeddedTitusMaster(Builder builder) {
        this.config = new DefaultSettableConfig();

        this.properties = builder.props;
        this.config.setProperties(builder.props);
        this.apiPort = builder.apiPort;
        this.grpcPort = builder.grpcPort;
        this.enableREST = builder.enableREST;
        this.enableDisruptionBudget = builder.enableDisruptionBudget;
        this.cellName = builder.cellName;
        this.masterDescription = new MasterDescription(
                "embedded_titus_master", "192.168.0.1", builder.apiPort, "api/postjobstatus",
                System.currentTimeMillis()
        );
        this.jobStore = builder.v3JobStore == null ? new InMemoryJobStore() : builder.v3JobStore;
        this.cassandraJobStore = builder.cassandraJobStore;

        String resourceDir = TitusMaster.class.getClassLoader().getResource("static").toExternalForm();

        Properties embeddedProperties = new Properties();
        embeddedProperties.put("governator.jetty.embedded.webAppResourceBase", resourceDir);
        embeddedProperties.put("titus.master.cellName", cellName);
        if (enableDisruptionBudget) {
            embeddedProperties.put("titusMaster.jobManager.features.disruptionBudget.featureEnabled", "true");
            embeddedProperties.put("titusMaster.jobManager.features.disruptionBudget.whiteList", ".*");
        }
        config.setProperties(embeddedProperties);

        this.embeddedKubeCluster = builder.embeddedKubeCluster;
    }

    public EmbeddedTitusMaster boot() {
        Stopwatch timer = Stopwatch.createStarted();
        logger.info("Starting Titus Master");

        Module embeddedKubeModule;
        if (embeddedKubeCluster == null) {
            embeddedKubeModule = new AbstractModule() {
                @Override
                protected void configure() {
                }
            };
        } else {
            embeddedKubeModule = new EmbeddedKubeModule(embeddedKubeCluster);
        }

        injector = InjectorBuilder.fromModules(
                Modules.override(new TitusRuntimeModule(false)).with(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
                        bind(Registry.class).toInstance(new DefaultRegistry());
                    }
                }),
                embeddedKubeModule,
                Modules.override(new TitusMasterModule(enableREST, TitusMasterModule.Mode.EMBEDDED_KUBE))
                        .with(new AbstractModule() {
                                  @Override
                                  protected void configure() {
                                      bind(InstanceCloudConnector.class).toInstance(new NoOpInstanceCloudConnector());
                                      bind(MasterDescription.class).toInstance(masterDescription);
                                      bind(MasterMonitor.class).to(LocalMasterMonitor.class);

                                      bind(AppScalePolicyStore.class).to(InMemoryPolicyStore.class);

                                      bind(LoadBalancerStore.class).to(InMemoryLoadBalancerStore.class);
                                      bind(LoadBalancerConnector.class).to(NoOpLoadBalancerConnector.class);
                                      bind(LoadBalancerJobValidator.class).to(NoOpLoadBalancerJobValidator.class);
                                  }

                                  @Provides
                                  @Singleton
                                  public JobStore getJobStore(TitusRuntime titusRuntime) {
                                      if (!cassandraJobStore) {
                                          return jobStore;
                                      }
                                      try {
                                          JobStore jobStore = EmbeddedCassandraStoreFactory.newBuilder()
                                                  .withTitusRuntime(titusRuntime)
                                                  .build()
                                                  .getJobStore();
                                          return jobStore;
                                      } catch (Throwable e) {
                                          e.printStackTrace();
                                          return null;
                                      }
                                  }
                              }
                        ),
                newJettyModule(),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindApplicationConfigurationOverride().toInstance(config);
                    }
                }).createInjector();

        if (grpcPort <= 0) {
            grpcPort = getGrpcPort();
            config.setProperty("titus.master.grpcServer.port", "" + grpcPort);
        }

        injector.getInstance(ContainerEventBus.class).submitInOrder(new ContainerEventBus.ContainerStartedEvent());

        injector.getInstance(LeaderActivator.class).becomeLeader();
        injector.getInstance(AuditLogService.class).auditLogEvents().subscribe(auditLogs::add);

        if (enableREST) {
            // Since jetty API server is run on a separate thread, it may not be ready yet
            // We do not have better way, but call it until it replies.
            getClient().findAllApplicationSLA().retryWhen(attempts -> {
                        return attempts.zipWith(Observable.range(1, 5), (n, i) -> i).flatMap(i -> {
                            return Observable.timer(i, TimeUnit.SECONDS);
                        });
                    }
            ).timeout(30, TimeUnit.SECONDS).toBlocking().firstOrDefault(null);
        }

        logger.info("Embedded TitusMaster started in {}ms", timer.elapsed(TimeUnit.MILLISECONDS));

        return this;
    }

    private Module newJettyModule() {
        if (!enableREST) {
            return Modules.EMPTY_MODULE;
        }
        return new EmbeddedJettyModule(apiPort);
    }

    public void shutdown() {
        if (grpcChannel != null) {
            grpcChannel.shutdown();
            grpcChannel = null;
        }
        if (injector != null) {
            injector.close();
        }
    }

    public DefaultSettableConfig getConfig() {
        return config;
    }

    public TitusMasterClient getClient() {
        int jettyPort = injector.getInstance(JettyModule.JettyRunner.class).getLocalPort();
        return new ReactorTitusMasterClient("127.0.0.1", jettyPort);
    }

    public HealthStub getHealthClient() {
        HealthStub client = HealthGrpc.newStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public SupervisorServiceBlockingStub getSupervisorBlockingGrpcClient() {
        SupervisorServiceBlockingStub client = SupervisorServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public SchedulerServiceGrpc.SchedulerServiceBlockingStub getV3BlockingSchedulerClient() {
        SchedulerServiceGrpc.SchedulerServiceBlockingStub client = SchedulerServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public JobManagementServiceStub getV3GrpcClient() {
        JobManagementServiceStub client = JobManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        JobManagementServiceBlockingStub client = JobManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient() {
        AgentManagementServiceGrpc.AgentManagementServiceStub client = AgentManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient() {
        AgentManagementServiceGrpc.AgentManagementServiceBlockingStub client = AgentManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient() {
        AutoScalingServiceGrpc.AutoScalingServiceStub client = AutoScalingServiceGrpc.newStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient() {
        LoadBalancerServiceGrpc.LoadBalancerServiceStub client = LoadBalancerServiceGrpc.newStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub getJobActivityHistoryGrpcClient() {
        JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub client = JobActivityHistoryServiceGrpc.newStub(getOrCreateGrpcChannel());
        return TestKitGrpcClientErrorUtils.attachCallHeaders(client);
    }

    public EvictionServiceGrpc.EvictionServiceBlockingStub getBlockingGrpcEvictionClient() {
        return EvictionServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
    }

    public MachineServiceGrpc.MachineServiceBlockingStub getBlockingGrpcMachineClient() {
        return MachineServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
    }

    private ManagedChannel getOrCreateGrpcChannel() {
        if (grpcChannel == null) {
            this.grpcChannel = ManagedChannelBuilder.forAddress("127.0.0.1", getGrpcPort())
                    .usePlaintext()
                    .build();
        }
        return grpcChannel;
    }

    public JobStore getJobStore() {
        return jobStore;
    }

    public void reboot() {
        shutdown();
        boot();
    }

    public <T> T getInstance(Class<T> type) {
        return injector.getInstance(type);
    }

    public <T> T getInstance(Key<T> type) {
        return injector.getInstance(type);
    }

    public static Builder aTitusMaster() {
        return new Builder();
    }

    public String getCellName() {
        return cellName;
    }

    public int getGrpcPort() {
        Preconditions.checkNotNull(injector, "TitusMaster not started yet");
        return injector.getInstance(TitusMasterGrpcServer.class).getGrpcPort();
    }

    public EmbeddedKubeCluster getEmbeddedKubeCluster() {
        return embeddedKubeCluster;
    }

    public Builder toBuilder() {
        return new Builder()
                .withApiPort(apiPort)
                .withGrpcPort(grpcPort)
                .withCellName(cellName)
                .withEmbeddedKubeCluster(embeddedKubeCluster)
                .withProperties(properties)
                .withEnableDisruptionBudget(enableDisruptionBudget)
                .withV3JobStore(jobStore);
    }

    public static class Builder {

        private Properties props = new Properties();

        public String cellName = CELL_NAME;
        private int apiPort;
        private int grpcPort;
        private boolean enableREST = true;
        private boolean enableDisruptionBudget;

        private JobStore v3JobStore;
        private boolean cassandraJobStore;
        private AgentStore agentStore;
        private EmbeddedKubeCluster embeddedKubeCluster;
        private SystemDisruptionBudgetDescriptor systemDisruptionBudgetDescriptor;

        public Builder() {
            props.put("titusMaster.job.configuration.defaultSecurityGroups", "sg-12345,sg-34567");
            props.put("titusMaster.job.configuration.defaultIamRole", "iam-12345");
        }

        public Builder withCellName(String cellName) {
            this.cellName = cellName;
            return this;
        }

        public Builder withApiPort(int apiPort) {
            this.apiPort = apiPort;
            return this;
        }

        public Builder withGrpcPort(int grpcPort) {
            this.grpcPort = grpcPort;
            return this;
        }

        public Builder withEnableREST(boolean enableREST) {
            this.enableREST = enableREST;
            return this;
        }

        public Builder withEnableDisruptionBudget(boolean enableDisruptionBudget) {
            this.enableDisruptionBudget = enableDisruptionBudget;
            return this;
        }

        public Builder withProperty(String name, String value) {
            props.put(name, value);
            return this;
        }

        public Builder withProperties(Properties properties) {
            props.putAll(properties);
            return this;
        }

        public Builder withSystemDisruptionBudgetDescriptor(SystemDisruptionBudgetDescriptor systemDisruptionBudgetDescriptor) {
            this.systemDisruptionBudgetDescriptor = systemDisruptionBudgetDescriptor;
            return this;
        }

        public Builder withV3JobStore(JobStore jobStore) {
            this.v3JobStore = jobStore;
            return this;
        }

        public Builder withCassadraV3JobStore() {
            this.cassandraJobStore = true;
            return this;
        }

        public Builder withEmbeddedKubeCluster(EmbeddedKubeCluster embeddedKubeCluster) {
            this.embeddedKubeCluster = embeddedKubeCluster;
            return this;
        }

        public EmbeddedTitusMaster build() {
            if (systemDisruptionBudgetDescriptor != null) {
                try {
                    props.put(
                            ArchaiusSystemDisruptionBudgetResolver.PROPERTY_KEY,
                            ObjectMappers.storeMapper().writeValueAsString(systemDisruptionBudgetDescriptor)
                    );
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
            }

            props.put("titus.master.audit.auditLogFolder", "build/auditLogs");
            props.put("titus.master.apiport", Integer.toString(apiPort));
            props.put("titus.master.grpcServer.port", Integer.toString(grpcPort));

            return new EmbeddedTitusMaster(this);
        }
    }
}
