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

package com.netflix.titus.federation.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.federation.service.router.ApplicationCellRouter;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FallbackJobServiceGatewayTest {
    private static final int TASKS_IN_GENERATED_JOBS = 10;
    private static final long GRPC_REQUEST_TIMEOUT_MS = 1_000L;
    private static final long GRPC_PRIMARY_FALLBACK_TIMEOUT_MS = 100L;

    @Rule
    public GrpcServerRule remoteFederationRule = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();
    private final TitusFederationConfiguration fedConfig = mock(TitusFederationConfiguration.class);

    private String stackName;
    private AggregatingJobServiceGateway aggregatingJobServiceGateway;
    private RemoteJobServiceGateway remoteJobServiceGateway;
    private FallbackJobServiceGateway fallbackJobServiceGateway;
    private List<Cell> cells;
    private Map<Cell, GrpcServerRule> cellToServiceMap;
    private TestClock clock;
    private ServiceDataGenerator dataGenerator;

    @Before
    public void setUp() {
        stackName = UUID.randomUUID().toString();

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(GRPC_REQUEST_TIMEOUT_MS);
        when(grpcConfiguration.getPrimaryFallbackTimeoutMs()).thenReturn(GRPC_PRIMARY_FALLBACK_TIMEOUT_MS);

        when(fedConfig.getStack()).thenReturn(stackName);
        when(fedConfig.getCells()).thenReturn("one=1;two=2");
        when(fedConfig.getRoutingRules()).thenReturn("one=(app1.*|app2.*);two=(app3.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(fedConfig);
        ApplicationCellRouter cellRouter = new ApplicationCellRouter(cellInfoResolver, fedConfig);
        cells = cellInfoResolver.resolve();
        cellToServiceMap = ImmutableMap.of(
                cells.get(0), cellOne,
                cells.get(1), cellTwo
        );

        RemoteFederationConnector fedConnector = mock(RemoteFederationConnector.class);
        when(fedConnector.getChannel()).thenReturn(remoteFederationRule.getChannel());

        CellConnector cellConnector = mock(CellConnector.class);
        when(cellConnector.getChannels()).thenReturn(cellToServiceMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, cellPairEntry -> cellPairEntry.getValue().getChannel()))
        );
        when(cellConnector.getChannelForCell(any(Cell.class))).thenAnswer(invocation ->
                Optional.ofNullable(cellToServiceMap.get(invocation.<Cell>getArgument(0)))
                        .map(GrpcServerRule::getChannel)
        );

        final AggregatingCellClient aggregatingCellClient = new AggregatingCellClient(cellConnector);
        aggregatingJobServiceGateway = new AggregatingJobServiceGateway(
                grpcConfiguration,
                fedConfig,
                cellConnector,
                cellRouter,
                aggregatingCellClient,
                new AggregatingJobManagementServiceHelper(aggregatingCellClient, grpcConfiguration)
        );

        remoteJobServiceGateway = new RemoteJobServiceGateway(fedConfig, fedConnector, cellRouter, grpcConfiguration);
        fallbackJobServiceGateway = new FallbackJobServiceGateway(fedConfig, remoteJobServiceGateway, aggregatingJobServiceGateway);

        clock = Clocks.test();
        dataGenerator = new ServiceDataGenerator(clock, TASKS_IN_GENERATED_JOBS);
    }

    @BeforeEach
    public void beforeEach() {
        when(fedConfig.isRemoteFederationEnabled()).thenReturn(false);
    }

    @Test
    public void createJobWithFallbackOnUnimplemented() {
        createJobWithFallbackFromRemoteJobManagementService(new RemoteJobManagementServiceWithUnimplementedInterface());
    }

    @Test
    public void createJobWithFallbackOnTimeout() {
        createJobWithFallbackFromRemoteJobManagementService(new RemoteJobManagementServiceWithSlowMethods());
    }

    private void createJobWithFallbackFromRemoteJobManagementService(RemoteJobManagementService remoteJobManagementService) {
        CellWithCachedJobsService cachedJobsService = new CellWithCachedJobsService(cells.get(0).getName());
        cellOne.getServiceRegistry().addService(cachedJobsService);
        remoteFederationRule.getServiceRegistry().addService(remoteJobManagementService);
        JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                .setApplicationName("app1")
                .build();

        // Prove fallback is NOT happening

        long initialCreateCount = remoteJobManagementService.createCount.get();
        assertThat(initialCreateCount).isEqualTo(0);

        Observable<String> createObservable =
                fallbackJobServiceGateway.createJob(jobDescriptor, JobManagerConstants.UNDEFINED_CALL_METADATA);

        String jobId = createObservable.toBlocking().first();
        Optional<JobDescriptor> createdJob = cachedJobsService.getCachedJob(jobId);
        assertThat(createdJob).isPresent();
        assertThat(remoteJobManagementService.createCount.get()).isEqualTo(initialCreateCount);

        // Prove fallback IS happening

        when(fedConfig.isRemoteFederationEnabled()).thenReturn(true);

        initialCreateCount = remoteJobManagementService.createCount.get();
        assertThat(initialCreateCount).isEqualTo(0);

        Observable<String> fallbackObservable =
                fallbackJobServiceGateway.createJob(jobDescriptor, JobManagerConstants.UNDEFINED_CALL_METADATA);

        jobId = fallbackObservable.toBlocking().first();
        createdJob = cachedJobsService.getCachedJob(jobId);
        assertThat(createdJob).isPresent();
        assertThat(remoteJobManagementService.createCount.get()).isEqualTo(initialCreateCount + 1);
    }
}

