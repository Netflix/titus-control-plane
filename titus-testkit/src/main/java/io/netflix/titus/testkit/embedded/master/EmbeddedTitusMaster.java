/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.embedded.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.util.Modules;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.ext.cassandra.testkit.store.EmbeddedCassandraStoreFactory;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.api.appscale.store.AppScalePolicyStore;
import io.netflix.titus.api.audit.model.AuditLogEvent;
import io.netflix.titus.api.audit.service.AuditLogService;
import io.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.connector.cloud.noop.NoOpLoadBalancerConnector;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.model.sanitizer.NoOpLoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.grpc.V3HeaderInterceptor;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import io.netflix.titus.common.util.guice.ContainerEventBus;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.TitusMaster;
import io.netflix.titus.master.TitusMasterModule;
import io.netflix.titus.master.TitusRuntimeModule;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.agent.store.InMemoryAgentStore;
import io.netflix.titus.master.cluster.LeaderActivator;
import io.netflix.titus.master.cluster.LeaderElector;
import io.netflix.titus.master.endpoint.common.SchedulerUtil;
import io.netflix.titus.master.job.worker.WorkerStateMonitor;
import io.netflix.titus.master.job.worker.internal.DefaultWorkerStateMonitor;
import io.netflix.titus.master.master.MasterDescription;
import io.netflix.titus.master.master.MasterMonitor;
import io.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import io.netflix.titus.master.scheduler.SchedulingService;
import io.netflix.titus.master.store.V2StorageProvider;
import io.netflix.titus.runtime.store.v3.memory.InMemoryJobStore;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import io.netflix.titus.runtime.store.v3.memory.InMemoryPolicyStore;
import io.netflix.titus.testkit.client.DefaultTitusMasterClient;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedAgentConfiguration;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.connector.local.SimulatedLocalInstanceCloudConnector;
import io.netflix.titus.testkit.embedded.cloud.connector.local.SimulatedLocalMesosSchedulerDriver;
import io.netflix.titus.testkit.embedded.cloud.connector.local.SimulatedLocalMesosSchedulerDriverFactory;
import io.netflix.titus.testkit.embedded.cloud.connector.remote.CloudSimulatorResolver;
import io.netflix.titus.testkit.embedded.cloud.connector.remote.SimulatedRemoteInstanceCloudConnector;
import io.netflix.titus.testkit.embedded.cloud.connector.remote.SimulatedRemoteMesosSchedulerDriverFactory;
import io.netflix.titus.testkit.util.NetworkExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Run TitusMaster server with mocked external integrations (mesos, storage).
 */
public class EmbeddedTitusMaster {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedTitusMaster.class);

    private final Properties properties;
    private final DefaultSettableConfig config;
    private final int apiPort;
    private final int grpcPort;
    private final MasterDescription masterDescription;

    private final V2StorageProvider storageProvider;
    private final JobStore jobStore;
    private final boolean cassandraJobStore;
    private final AgentStore agentStore;

    private final SimulatedCloud simulatedCloud;
    private final InstanceCloudConnector cloudInstanceConnector;
    private final MesosSchedulerDriverFactory mesosSchedulerDriverFactory;

    private LifecycleInjector injector;
    private final List<AuditLogEvent> auditLogs = new CopyOnWriteArrayList<>();
    private DefaultWorkerStateMonitor workerStateMonitor;

    private ManagedChannel grpcChannel;

    private EmbeddedTitusMaster(Builder builder) {
        this.config = new DefaultSettableConfig();

        this.properties = builder.props;
        this.config.setProperties(builder.props);
        this.apiPort = builder.apiPort;
        this.grpcPort = builder.grpcPort;
        this.masterDescription = new MasterDescription(
                "embedded_titus_master", "192.168.0.1", builder.apiPort, "api/postjobstatus",
                System.currentTimeMillis()
        );
        this.storageProvider = builder.v2JobStore;
        this.jobStore = builder.v3JobStore == null ? new InMemoryJobStore() : builder.v3JobStore;
        this.cassandraJobStore = builder.cassandraJobStore;
        this.agentStore = builder.agentStore == null ? new InMemoryAgentStore() : builder.agentStore;

        String resourceDir = TitusMaster.class.getClassLoader().getResource("static").toExternalForm();

        Properties embeddedProperties = new Properties();
        embeddedProperties.put("governator.jetty.embedded.port", apiPort);
        embeddedProperties.put("governator.jetty.embedded.webAppResourceBase", resourceDir);
        config.setProperties(embeddedProperties);

        if (builder.remoteCloud == null) {
            this.simulatedCloud = builder.simulatedCloud == null ? new SimulatedCloud() : builder.simulatedCloud;
            this.cloudInstanceConnector = new SimulatedLocalInstanceCloudConnector(simulatedCloud);
            this.mesosSchedulerDriverFactory = new SimulatedLocalMesosSchedulerDriverFactory(simulatedCloud);
        } else {
            this.simulatedCloud = null;

            CloudSimulatorResolver connectorConfiguration = () -> builder.remoteCloud;
            this.cloudInstanceConnector = new SimulatedRemoteInstanceCloudConnector(connectorConfiguration);
            this.mesosSchedulerDriverFactory = new SimulatedRemoteMesosSchedulerDriverFactory(connectorConfiguration);

        }
        if (simulatedCloud != null) {
            builder.agentClusters.forEach(simulatedCloud::addInstanceGroup);
        }
    }

    public EmbeddedTitusMaster boot() {
        Stopwatch timer = Stopwatch.createStarted();
        logger.info("Starting Titus Master");

        injector = InjectorBuilder.fromModules(
                Modules.override(new TitusRuntimeModule()).with(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
                        bind(Registry.class).toInstance(new DefaultRegistry());
                    }
                }),
                Modules.override(new TitusMasterModule())
                        .with(new AbstractModule() {
                                  @Override
                                  protected void configure() {
                                      bind(InstanceCloudConnector.class).toInstance(cloudInstanceConnector);
                                      bind(MesosSchedulerDriverFactory.class).toInstance(mesosSchedulerDriverFactory);

                                      bind(LeaderElector.class).to(EmbeddedLeaderElector.class);
                                      bind(MasterDescription.class).toInstance(masterDescription);
                                      bind(MasterMonitor.class).to(EmbeddedMasterMonitor.class);
                                      bind(V2StorageProvider.class).toInstance(storageProvider);
                                      bind(AgentStore.class).toInstance(agentStore);

                                      bind(VirtualMachineMasterService.class).to(EmbeddedVirtualMachineMasterService.class);

                                      bind(AppScalePolicyStore.class).to(InMemoryPolicyStore.class);

                                      bind(LoadBalancerStore.class).to(InMemoryLoadBalancerStore.class);
                                      bind(LoadBalancerConnector.class).to(NoOpLoadBalancerConnector.class);
                                      bind(LoadBalancerJobValidator.class).to(NoOpLoadBalancerJobValidator.class);
                                  }

                                  @Provides
                                  @Singleton
                                  public SimulatedAgentConfiguration getSimulatedAgentConfiguration(ConfigProxyFactory factory) {
                                      return factory.newProxy(SimulatedAgentConfiguration.class);
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
                new Archaius2JettyModule(),
                new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindApplicationConfigurationOverride().toInstance(config);
                    }
                }).createInjector();

        injector.getInstance(ContainerEventBus.class).submitInOrder(new ContainerEventBus.ContainerStartedEvent());

        injector.getInstance(LeaderActivator.class).becomeLeader();
        injector.getInstance(AuditLogService.class).auditLogEvents().subscribe(auditLogs::add);

        // Since jetty API server is run on a separate thread, it may not be ready yet
        // We do not have better way, but call it until it replies.
        getClient().findAllJobs().retryWhen(attempts -> {
                    return attempts.zipWith(Observable.range(1, 5), (n, i) -> i).flatMap(i -> {
                        return Observable.timer(i, TimeUnit.SECONDS);
                    });
                }
        ).timeout(30, TimeUnit.SECONDS).toBlocking().firstOrDefault(null);

        workerStateMonitor = injector.getInstance(DefaultWorkerStateMonitor.class);
        logger.info("Embedded TitusMaster started in {}ms", timer.elapsed(TimeUnit.MILLISECONDS));

        return this;
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

    public void addAgentCluster(SimulatedTitusAgentCluster agentCluster) {
        simulatedCloud.addInstanceGroup(agentCluster);
    }

    public SimulatedCloud getSimulatedCloud() {
        return simulatedCloud;
    }

    public V2StorageProvider getStorageProvider() {
        return storageProvider;
    }

    public DefaultSettableConfig getConfig() {
        return config;
    }

    public TitusMasterClient getClient() {
        return new DefaultTitusMasterClient("127.0.0.1", apiPort);
    }

    public JobManagementServiceStub getV3GrpcClient() {
        JobManagementServiceStub client = JobManagementServiceGrpc.newStub(getOrCreateGrpcChannel());
        return V3HeaderInterceptor.attachCallerId(client, "integrationTest");
    }

    public JobManagementServiceBlockingStub getV3BlockingGrpcClient() {
        JobManagementServiceBlockingStub client = JobManagementServiceGrpc.newBlockingStub(getOrCreateGrpcChannel());
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
            this.grpcChannel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                    .usePlaintext(true)
                    .build();
        }
        return grpcChannel;
    }

    public RxEventBus getEventBus() {
        return injector.getInstance(RxEventBus.class);
    }

    public JobStore getJobStore() {
        return jobStore;
    }

    public Observable<TaskExecutorHolder> observeLaunchedTasks() {
        return getMesosSchedulerDriver().observeLaunchedTasks();
    }

    public void reboot() {
        shutdown();
        boot();
    }

    public SimulatedLocalMesosSchedulerDriver getMesosSchedulerDriver() {
        return ((EmbeddedVirtualMachineMasterService) injector.getInstance(VirtualMachineMasterService.class)).getSimulatedMesosDriver();
    }

    public <T> T getInstance(Class<T> type) {
        return injector.getInstance(type);
    }

    public List<TaskAssignmentResult> reportForTask(String taskId) {
        return SchedulerUtil.blockAndGetTaskAssignmentFailures(injector.getInstance(SchedulingService.class), taskId);
    }

    public WorkerStateMonitor getWorkerStateMonitor() {
        return workerStateMonitor;
    }

    public static Builder aTitusMaster() {
        return new Builder();
    }

    public Observable<TaskExecutorHolder> awaitTaskExecutorHolderOf(String taskId) {
        return observeLaunchedTasks().compose(ObservableExt.head(() -> simulatedCloud.getAgentInstanceGroups().stream()
                .flatMap(c -> c.getAgents().stream())
                .flatMap(a -> a.findTaskById(taskId).map(Collections::singletonList).orElse(Collections.emptyList()).stream())
                .limit(1)
                .collect(Collectors.toList())));
    }

    public int getApiPort() {
        return apiPort;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public Builder toBuilder() {
        return new Builder()
                .withApiPort(apiPort)
                .withGrpcPort(grpcPort)
                .withSimulatedCloud(simulatedCloud)
                .withProperties(properties)
                .withV3JobStore(jobStore)
                .withV2JobStore(storageProvider);
    }

    public static class Builder {

        private Properties props = new Properties();
        private int apiPort;
        private int grpcPort;

        private V2StorageProvider v2JobStore;
        private JobStore v3JobStore;
        private boolean cassandraJobStore;
        private AgentStore agentStore;
        private List<SimulatedTitusAgentCluster> agentClusters = new ArrayList<>();
        private SimulatedCloud simulatedCloud;
        private Pair<String, Integer> remoteCloud;

        public Builder() {
            props.put("titusMaster.job.configuration.defaultSecurityGroups", "sg-12345,sg-34567");
            props.put("titusMaster.job.configuration.defaultIamRole", "iam-12345");
        }

        public Builder withApiPort(int apiPort) {
            this.apiPort = apiPort;
            return this;
        }

        public Builder withGrpcPort(int grpcPort) {
            this.grpcPort = grpcPort;
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

        public Builder withV2JobStore(V2StorageProvider storageProvider) {
            this.v2JobStore = storageProvider;
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

        public Builder withAgentStore(AgentStore agentStore) {
            this.agentStore = agentStore;
            return this;
        }

        public Builder withAgentCluster(SimulatedTitusAgentCluster agentCluster) {
            agentClusters.add(agentCluster);
            return this;
        }

        public Builder withAgentCluster(SimulatedTitusAgentCluster.Builder builder) {
            if (simulatedCloud == null) {
                this.simulatedCloud = new SimulatedCloud();
            }
            return withAgentCluster(builder
                    .withComputeResources(simulatedCloud.getComputeResources())
                    .withContainerPlayersManager(simulatedCloud.getContainerPlayersManager())
                    .build()
            );
        }

        public Builder withSimulatedCloud(SimulatedCloud simulatedCloud) {
            Preconditions.checkState(this.simulatedCloud == null, "Simulated cloud already configured");
            Preconditions.checkState(remoteCloud == null, "Remote simulated cloud already configured");
            this.simulatedCloud = simulatedCloud;
            return this;
        }

        public void withRemoteCloud(String hostAddress, int grpcPort) {
            Preconditions.checkState(this.simulatedCloud == null, "Simulated cloud already configured");

            this.remoteCloud = Pair.of(hostAddress, grpcPort);
        }

        public EmbeddedTitusMaster build() {
            if (apiPort == 0) {
                apiPort = NetworkExt.findUnusedPort();
            }
            grpcPort = grpcPort == 0 ? NetworkExt.findUnusedPort() : grpcPort;

            props.put("titus.master.audit.auditLogFolder", "build/auditLogs");
            props.put("titus.master.apiport", Integer.toString(apiPort));
            props.put("titus.master.grpcServer.port", Integer.toString(grpcPort));

            if (v2JobStore == null) {
                v2JobStore = new EmbeddedStorageProvider();
            }

            return new EmbeddedTitusMaster(this);
        }
    }
}