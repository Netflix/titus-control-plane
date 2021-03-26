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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.federation.service.router.ApplicationCellRouter;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.observers.AssertableSubscriber;
import rx.subjects.PublishSubject;

import static com.netflix.titus.api.jobmanager.service.JobManagerConstants.UNDEFINED_CALL_METADATA;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toGrpcPage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingJobServiceGatewayWithSingleCellTest {
    private static final int TASKS_IN_GENERATED_JOBS = 10;

    @Rule
    public final GrpcServerRule cell = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule federation = new GrpcServerRule().directExecutor();

    private String stackName;
    private AggregatingJobServiceGateway service;
    private Map<Cell, GrpcServerRule> cellToServiceMap;
    private TestClock clock;
    private ServiceDataGenerator dataGenerator;

    @Before
    public void setUp() {
        stackName = UUID.randomUUID().toString();

        GrpcConfiguration grpcClientConfiguration = mock(GrpcConfiguration.class);
        when(grpcClientConfiguration.getRequestTimeoutMs()).thenReturn(1000L);

        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getStack()).thenReturn(stackName);
        when(titusFederationConfiguration.getCells()).thenReturn("one=1");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("one=(app1.*|app2.*);two=(app3.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);
        ApplicationCellRouter cellRouter = new ApplicationCellRouter(cellInfoResolver, titusFederationConfiguration);
        List<Cell> cells = cellInfoResolver.resolve();
        cellToServiceMap = ImmutableMap.of(cells.get(0), cell);

        FederationConnector fedConnector = mock(FederationConnector.class);
        when(fedConnector.getChannel()).thenReturn(Optional.of(federation.getChannel()));

        CellConnector cellConnector = mock(CellConnector.class);
        when(cellConnector.getChannels()).thenReturn(cellToServiceMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, cellPairEntry -> cellPairEntry.getValue().getChannel()))
        );
        when(cellConnector.getChannelForCell(any(Cell.class))).thenAnswer(invocation ->
                Optional.ofNullable(cellToServiceMap.get(invocation.<Cell>getArgument(0)))
                        .map(GrpcServerRule::getChannel)
        );

        final AggregatingCellClient aggregatingCellClient = new AggregatingCellClient(cellConnector);
        final AnonymousCallMetadataResolver anonymousCallMetadataResolver = new AnonymousCallMetadataResolver();
        service = new AggregatingJobServiceGateway(
                grpcClientConfiguration,
                titusFederationConfiguration,
                fedConnector,
                cellConnector,
                cellRouter,
                aggregatingCellClient,
                new AggregatingJobManagementServiceHelper(aggregatingCellClient, grpcClientConfiguration)
        );

        clock = Clocks.test();
        dataGenerator = new ServiceDataGenerator(clock, TASKS_IN_GENERATED_JOBS);
    }

    @Test
    public void findJobsAddsStackName() {
        Random random = new Random();
        final List<Job> cellSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cellSnapshot.addAll(dataGenerator.newBatchJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob));
            cellSnapshot.addAll(dataGenerator.newServiceJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cell.getServiceRegistry().addService(new CellWithFixedJobsService(cellSnapshot, PublishSubject.create()));

        JobQuery query = JobQuery.newBuilder()
                .setPage(toGrpcPage(Page.unlimited()))
                .build();

        final AssertableSubscriber<JobQueryResult> testSubscriber = service.findJobs(query, UNDEFINED_CALL_METADATA).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors().assertCompleted();
        testSubscriber.assertValueCount(1);
        final List<JobQueryResult> results = testSubscriber.getOnNextEvents();
        assertThat(results).hasSize(1);

        // expect stackName to have changed
        List<Job> expected = cellSnapshot.stream()
                .sorted(JobManagerCursors.jobCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(results.get(0).getItemsList()).containsExactlyElementsOf(expected);
    }

    @Test
    public void findTasksMergesAllCellsIntoSingleResult() {
        List<Task> cellSnapshot = new ArrayList<>();
        // 10 jobs on each cell with TASKS_IN_GENERATED_JOBS tasks each
        for (int i = 0; i < 5; i++) {
            cellSnapshot.addAll(dataGenerator.newBatchJobWithTasks());
            cellSnapshot.addAll(dataGenerator.newServiceJobWithTasks());
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cell.getServiceRegistry().addService(new CellWithFixedTasksService(cellSnapshot));

        TaskQuery query = TaskQuery.newBuilder()
                .setPage(toGrpcPage(Page.unlimited()))
                .build();

        final AssertableSubscriber<TaskQueryResult> testSubscriber = service.findTasks(query, UNDEFINED_CALL_METADATA).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors().assertCompleted();
        testSubscriber.assertValueCount(1);
        final List<TaskQueryResult> results = testSubscriber.getOnNextEvents();
        assertThat(results).hasSize(1);

        // expect stackName to have changed
        List<Task> expected = cellSnapshot.stream()
                .sorted(JobManagerCursors.taskCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(results.get(0).getItemsList()).containsExactlyElementsOf(expected);
    }

    private Job withStackName(Job job) {
        JobDescriptor jobDescriptor = job.getJobDescriptor().toBuilder()
                .putAttributes("titus.stack", stackName)
                .build();
        return job.toBuilder().setJobDescriptor(jobDescriptor).build();
    }

    private Task withStackName(Task task) {
        return task.toBuilder()
                .putTaskContext("titus.stack", stackName)
                .build();
    }
}

