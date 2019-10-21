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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.JobUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification.SnapshotEnd;
import com.netflix.titus.grpc.protogen.JobChangeNotification.TaskUpdate;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.observers.AssertableSubscriber;
import rx.subjects.PublishSubject;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;
import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_STACK;
import static com.netflix.titus.federation.service.ServiceTests.walkAllPages;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toGrpcPage;
import static io.grpc.Status.DEADLINE_EXCEEDED;
import static io.grpc.Status.INTERNAL;
import static io.grpc.Status.NOT_FOUND;
import static io.grpc.Status.UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingJobServiceGatewayTest {
    private static final JobStatus ACCEPTED_STATE = JobStatus.newBuilder().setState(JobStatus.JobState.Accepted).build();
    private static final JobStatus KILL_INITIATED_STATE = JobStatus.newBuilder().setState(JobStatus.JobState.KillInitiated).build();
    private static final JobStatus FINISHED_STATE = JobStatus.newBuilder().setState(JobStatus.JobState.Finished).build();
    private static final int TASKS_IN_GENERATED_JOBS = 10;
    private static final long GRPC_REQUEST_TIMEOUT_MS = 1_000L;

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellOneUpdates = PublishSubject.create();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellTwoUpdates = PublishSubject.create();

    private String stackName;
    private AggregatingJobServiceGateway service;
    private List<Cell> cells;
    private Map<Cell, GrpcServerRule> cellToServiceMap;
    private TestClock clock;
    private ServiceDataGenerator dataGenerator;

    @Before
    public void setUp() {
        stackName = UUID.randomUUID().toString();

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(GRPC_REQUEST_TIMEOUT_MS);

        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getStack()).thenReturn(stackName);
        when(titusFederationConfiguration.getCells()).thenReturn("one=1;two=2");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("one=(app1.*|app2.*);two=(app3.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);
        DefaultCellRouter cellRouter = new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration);
        cells = cellInfoResolver.resolve();
        cellToServiceMap = ImmutableMap.of(
                cells.get(0), cellOne,
                cells.get(1), cellTwo
        );

        CellConnector connector = mock(CellConnector.class);
        when(connector.getChannels()).thenReturn(cellToServiceMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, cellPairEntry -> cellPairEntry.getValue().getChannel()))
        );
        when(connector.getChannelForCell(any(Cell.class))).thenAnswer(invocation ->
                Optional.ofNullable(cellToServiceMap.get(invocation.<Cell>getArgument(0)))
                        .map(GrpcServerRule::getChannel)
        );

        final AggregatingCellClient aggregatingCellClient = new AggregatingCellClient(connector);
        final AnonymousCallMetadataResolver anonymousCallMetadataResolver = new AnonymousCallMetadataResolver();
        service = new AggregatingJobServiceGateway(
                grpcConfiguration,
                titusFederationConfiguration,
                connector,
                cellRouter,
                anonymousCallMetadataResolver,
                aggregatingCellClient,
                new AggregatingJobManagementServiceHelper(aggregatingCellClient, grpcConfiguration, anonymousCallMetadataResolver)
        );

        clock = Clocks.test();
        dataGenerator = new ServiceDataGenerator(clock, TASKS_IN_GENERATED_JOBS);
    }

    @After
    public void tearDown() {
        cellOneUpdates.onCompleted();
        cellTwoUpdates.onCompleted();
    }

    @Test
    public void findJobsMergesAllCellsIntoSingleResult() {
        Random random = new Random();
        final List<Job> cellOneSnapshot = new ArrayList<>();
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cellOneSnapshot.addAll(
                    dataGenerator.newBatchJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob)
            );
            cellTwoSnapshot.addAll(
                    dataGenerator.newBatchJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob)
            );
            cellOneSnapshot.addAll(
                    dataGenerator.newServiceJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob)
            );
            cellTwoSnapshot.addAll(
                    dataGenerator.newServiceJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob)
            );
            clock.advanceTime(1, TimeUnit.MINUTES);
        }

        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        JobQuery query = JobQuery.newBuilder()
                .setPage(toGrpcPage(Page.unlimited()))
                .build();

        final AssertableSubscriber<JobQueryResult> testSubscriber = service.findJobs(query).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors().assertCompleted();
        testSubscriber.assertValueCount(1);
        final List<JobQueryResult> results = testSubscriber.getOnNextEvents();
        assertThat(results).hasSize(1);

        // expect stackName to have changed
        List<Job> expected = Stream.concat(cellOneSnapshot.stream(), cellTwoSnapshot.stream())
                .sorted(JobManagerCursors.jobCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(results.get(0).getItemsList()).containsExactlyElementsOf(expected);
    }

    @Test
    public void findJobsEmptyPage() {
        Random random = new Random();
        final List<Job> cellOneSnapshot = new ArrayList<>();
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Iterables.addAll(cellOneSnapshot, Iterables.concat(
                    dataGenerator.newBatchJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob),
                    dataGenerator.newServiceJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob)
            ));
            Iterables.addAll(cellTwoSnapshot, Iterables.concat(
                    dataGenerator.newBatchJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob),
                    dataGenerator.newServiceJobs(random.nextInt(10), GrpcJobManagementModelConverters::toGrpcJob)
            ));
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        JobQuery query = JobQuery.newBuilder()
                .setPage(toGrpcPage(Page.empty()))
                .build();

        final AssertableSubscriber<JobQueryResult> testSubscriber = service.findJobs(query).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors().assertCompleted();
        testSubscriber.assertValueCount(1);
        final List<JobQueryResult> results = testSubscriber.getOnNextEvents();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getItemsList()).isEmpty();
        assertThat(results.get(0).getPagination().getHasMore()).isFalse();
    }

    @Test
    public void findJobsWithCursorPagination() {
        Pair<List<Job>, List<Job>> cellSnapshots = generateTestJobs();

        List<Job> allJobs = walkAllFindJobsPages(10);
        assertThat(allJobs).hasSize(cellSnapshots.getLeft().size() + cellSnapshots.getRight().size());

        // expect stackName to have changed
        List<Job> expected = Stream.concat(cellSnapshots.getLeft().stream(), cellSnapshots.getRight().stream())
                .sorted(JobManagerCursors.jobCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(allJobs).containsExactlyElementsOf(expected);
    }

    /**
     * Ensure that all items are still walked, even when pageSizes is smaller than the number of Cells. In other words,
     * make sure that the federation proxy is constantly alternating items to be returning from each Cell.
     */
    @Test
    public void findJobsWithSmallPageSizes() {
        final List<Job> cellOneSnapshot = new ArrayList<>();
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            clock.advanceTime(1, TimeUnit.SECONDS);
            cellOneSnapshot.addAll(dataGenerator.newBatchJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.SECONDS);
            cellTwoSnapshot.addAll(dataGenerator.newServiceJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.SECONDS);
            cellTwoSnapshot.addAll(dataGenerator.newBatchJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.SECONDS);
            cellTwoSnapshot.addAll(dataGenerator.newServiceJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        List<Job> allJobs = walkAllFindJobsPages(1);
        assertThat(allJobs).hasSize(cellOneSnapshot.size() + cellTwoSnapshot.size());

        // expect stackName to have changed
        List<Job> expected = CollectionsExt.merge(cellOneSnapshot, cellTwoSnapshot).stream()
                .sorted(JobManagerCursors.jobCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(allJobs).containsExactlyElementsOf(expected);
    }

    /**
     * When one of the cells is empty, ensure that findJobs (including pagination) is working as if only one Cell
     * existed.
     */
    @Test
    public void findJobsWithEmptyCell() {
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cellTwoSnapshot.addAll(dataGenerator.newBatchJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.SECONDS);
            cellTwoSnapshot.addAll(dataGenerator.newServiceJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        List<Job> allJobs = walkAllFindJobsPages(5);
        assertThat(allJobs).hasSize(cellTwoSnapshot.size());

        // expect stackName to have changed
        List<Job> expected = cellTwoSnapshot.stream()
                .sorted(JobManagerCursors.jobCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(allJobs).containsExactlyElementsOf(expected);
    }

    @Test
    public void findJobsWithAllEmptyCells() {
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellTwoUpdates.serialize()));
        List<Job> allJobs = walkAllFindJobsPages(5);
        assertThat(allJobs).isEmpty();
    }

    @Test
    public void findJobsWithFailingCell() {
        List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cellTwoSnapshot.addAll(dataGenerator.newBatchJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            cellTwoSnapshot.addAll(dataGenerator.newServiceJobs(5, GrpcJobManagementModelConverters::toGrpcJob));
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cellOne.getServiceRegistry().addService(new CellWithFailingJobManagementService());
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        JobQuery query = JobQuery.newBuilder()
                .setPage(toGrpcPage(Page.unlimited()))
                .build();

        final AssertableSubscriber<JobQueryResult> testSubscriber = service.findJobs(query).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertError(Status.INTERNAL.asRuntimeException().getClass());
        testSubscriber.assertNoValues();
    }

    @Test
    public void findJobsWithFieldFiltering() {
        Pair<List<Job>, List<Job>> cellSnapshots = generateTestJobs();
        List<Job> allJobs = walkAllPages(
                10,
                service::findJobs,
                page -> JobQuery.newBuilder()
                        .setPage(page)
                        .addFields("jobDescriptor.owner")
                        .build(),
                JobQueryResult::getPagination,
                JobQueryResult::getItemsList
        );
        assertThat(allJobs).hasSize(cellSnapshots.getLeft().size() + cellSnapshots.getRight().size());
        for (Job job : allJobs) {
            assertThat(job.getId()).isNotEmpty();
            assertThat(job.getJobDescriptor().getOwner().getTeamEmail()).isNotEmpty();
            assertThat(job.getStatus().getReasonMessage()).isEmpty();
            assertThat(job.getStatusHistoryList()).isEmpty();
        }
    }

    @Test
    public void findJob() {
        Random random = new Random();
        List<Job> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobs(10, GrpcJobManagementModelConverters::toGrpcJob));
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellTwoUpdates.serialize()));

        Job expected = withStackName(cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size())));
        AssertableSubscriber<Job> testSubscriber = service.findJob(expected.getId()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue(expected);
    }

    @Test
    public void findJobWithFailingCell() {
        Random random = new Random();
        List<Job> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobs(10, GrpcJobManagementModelConverters::toGrpcJob));
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFailingJobManagementService());

        Job expected = withStackName(cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size())));
        AssertableSubscriber<Job> testSubscriber = service.findJob(expected.getId()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue(expected);
    }

    @Test
    public void findJobErrors() {
        cellOne.getServiceRegistry().addService(new CellWithFailingJobManagementService(NOT_FOUND));
        cellTwo.getServiceRegistry().addService(new CellWithFailingJobManagementService(UNAVAILABLE));

        AssertableSubscriber<Job> testSubscriber = service.findJob("any").test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        // transient errors have higher precedence than not found
        testSubscriber.assertError(StatusRuntimeException.class);
        assertThat(Status.fromThrowable(testSubscriber.getOnErrorEvents().get(0))).isEqualTo(UNAVAILABLE);
    }

    @Test
    public void killJob() {
        Random random = new Random();
        List<Job> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobs(12, GrpcJobManagementModelConverters::toGrpcJob));
        List<Job> cellTwoSnapshot = new ArrayList<>(dataGenerator.newBatchJobs(7, GrpcJobManagementModelConverters::toGrpcJob));
        CellWithFixedJobsService cellOneService = new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize());
        CellWithFixedJobsService cellTwoService = new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize());
        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        Job killInCellOne = cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size()));
        Job killInCellTwo = cellTwoSnapshot.get(random.nextInt(cellTwoSnapshot.size()));
        assertThat(cellOneService.currentJobs()).containsKey(killInCellOne.getId());
        assertThat(cellTwoService.currentJobs()).containsKey(killInCellTwo.getId());

        AssertableSubscriber<Void> testSubscriber = service.killJob(killInCellOne.getId()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        assertThat(cellOneService.currentJobs()).doesNotContainKey(killInCellOne.getId());
        assertThat(cellTwoService.currentJobs()).doesNotContainKey(killInCellOne.getId());
        testSubscriber.unsubscribe();

        testSubscriber = service.killJob(killInCellTwo.getId()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        assertThat(cellOneService.currentJobs()).doesNotContainKey(killInCellTwo.getId());
        assertThat(cellTwoService.currentJobs()).doesNotContainKey(killInCellTwo.getId());
        testSubscriber.unsubscribe();
    }

    @Test
    public void singleJobUpdates() {
        Random random = new Random();
        List<String> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobs(12)).stream()
                .map(job -> job.getId())
                .collect(Collectors.toList());
        List<String> cellTwoSnapshot = new ArrayList<>(dataGenerator.newBatchJobs(7)).stream()
                .map(job -> job.getId())
                .collect(Collectors.toList());
        CellWithJobIds cellOneService = new CellWithJobIds(cellOneSnapshot);
        CellWithJobIds cellTwoService = new CellWithJobIds(cellTwoSnapshot);
        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        String cellOneJobId = cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size()));
        String cellTwoJobId = cellTwoSnapshot.get(random.nextInt(cellTwoSnapshot.size()));
        assertThat(cellOneService.containsCapacityUpdates(cellOneJobId)).isFalse();
        assertThat(cellTwoService.containsCapacityUpdates(cellOneJobId)).isFalse();

        JobCapacityUpdate cellOneUpdate = JobCapacityUpdate.newBuilder()
                .setJobId(cellOneJobId)
                .setCapacity(Capacity.newBuilder()
                        .setMax(1).setDesired(2).setMax(3)
                        .build())
                .build();
        AssertableSubscriber<Void> testSubscriber = service.updateJobCapacity(cellOneUpdate).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        assertThat(cellOneService.containsCapacityUpdates(cellOneJobId)).isTrue();
        assertThat(cellTwoService.containsCapacityUpdates(cellOneJobId)).isFalse();
        testSubscriber.unsubscribe();

        JobCapacityUpdate cellTwoUpdate = JobCapacityUpdate.newBuilder()
                .setJobId(cellTwoJobId)
                .setCapacity(Capacity.newBuilder()
                        .setMax(2).setDesired(2).setMax(2)
                        .build())
                .build();
        testSubscriber = service.updateJobCapacity(cellTwoUpdate).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        assertThat(cellOneService.containsCapacityUpdates(cellTwoJobId)).isFalse();
        assertThat(cellTwoService.containsCapacityUpdates(cellTwoJobId)).isTrue();
        testSubscriber.unsubscribe();
    }

    @Test
    public void killTask() {
        Random random = new Random();
        List<Task> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobWithTasks());
        List<Task> cellTwoSnapshot = new ArrayList<>(dataGenerator.newBatchJobWithTasks());
        CellWithFixedTasksService cellOneService = new CellWithFixedTasksService(cellOneSnapshot);
        CellWithFixedTasksService cellTwoService = new CellWithFixedTasksService(cellTwoSnapshot);
        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        Task killInCellOne = cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size()));
        Task killInCellTwo = cellTwoSnapshot.get(random.nextInt(cellTwoSnapshot.size()));
        assertThat(cellOneService.currentTasks()).containsKey(killInCellOne.getId());
        assertThat(cellTwoService.currentTasks()).containsKey(killInCellTwo.getId());

        TaskKillRequest cellOneRequest = TaskKillRequest.newBuilder()
                .setTaskId(killInCellOne.getId())
                .setShrink(false)
                .build();
        AssertableSubscriber<Void> testSubscriber = service.killTask(cellOneRequest).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        assertThat(cellOneService.currentTasks()).doesNotContainKey(killInCellOne.getId());
        assertThat(cellTwoService.currentTasks()).doesNotContainKey(killInCellOne.getId());
        testSubscriber.unsubscribe();

        TaskKillRequest cellTwoRequest = TaskKillRequest.newBuilder()
                .setTaskId(killInCellTwo.getId())
                .setShrink(false)
                .build();
        testSubscriber = service.killTask(cellTwoRequest).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
        testSubscriber.assertCompleted();
        assertThat(cellOneService.currentTasks()).doesNotContainKey(killInCellTwo.getId());
        assertThat(cellTwoService.currentTasks()).doesNotContainKey(killInCellTwo.getId());
        testSubscriber.unsubscribe();
    }

    @Test
    public void findTask() {
        Random random = new Random();
        List<Task> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobWithTasks());
        cellOne.getServiceRegistry().addService(new CellWithFixedTasksService(cellOneSnapshot));
        cellTwo.getServiceRegistry().addService(new CellWithFixedTasksService(Collections.emptyList()));

        Task expected = withStackName(cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size())));
        AssertableSubscriber<Task> testSubscriber = service.findTask(expected.getId()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue(expected);
    }

    @Test
    public void findTaskWithFailingCell() {
        Random random = new Random();
        List<Task> cellOneSnapshot = new ArrayList<>(dataGenerator.newServiceJobWithTasks());
        cellOne.getServiceRegistry().addService(new CellWithFixedTasksService(cellOneSnapshot));
        cellTwo.getServiceRegistry().addService(new CellWithFailingJobManagementService(DEADLINE_EXCEEDED));

        Task expected = withStackName(cellOneSnapshot.get(random.nextInt(cellOneSnapshot.size())));
        AssertableSubscriber<Task> testSubscriber = service.findTask(expected.getId()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue(expected);
    }

    @Test
    public void findTaskErrors() {
        cellOne.getServiceRegistry().addService(new CellWithFailingJobManagementService(INTERNAL));
        cellTwo.getServiceRegistry().addService(new CellWithFailingJobManagementService(UNAVAILABLE));

        AssertableSubscriber<Task> testSubscriber = service.findTask("any").test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        // unexpected errors have higher precedence than transient
        testSubscriber.assertError(StatusRuntimeException.class);
        assertThat(Status.fromThrowable(testSubscriber.getOnErrorEvents().get(0))).isEqualTo(INTERNAL);
    }

    @Test
    public void findTasksMergesAllCellsIntoSingleResult() {
        Pair<List<Task>, List<Task>> cellSnapshots = generateTestJobsWithTasks();

        List<Task> tasks = walkAllFindTasksPages(7);
        assertThat(tasks).hasSize(cellSnapshots.getLeft().size() + cellSnapshots.getRight().size());
        List<Task> expected = Stream.concat(cellSnapshots.getLeft().stream(), cellSnapshots.getRight().stream())
                .sorted(JobManagerCursors.taskCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(tasks).containsExactlyElementsOf(expected);
    }

    @Test
    public void findTasksWithFieldFiltering() {
        Pair<List<Task>, List<Task>> cellSnapshots = generateTestJobsWithTasks();

        List<Task> allTasks = walkAllPages(
                6,
                service::findTasks,
                page -> TaskQuery.newBuilder()
                        .setPage(page)
                        .addFields("jobId")
                        .build(),
                TaskQueryResult::getPagination,
                TaskQueryResult::getItemsList
        );
        assertThat(allTasks).hasSize(cellSnapshots.getLeft().size() + cellSnapshots.getRight().size());
        for (Task task : allTasks) {
            assertThat(task.getId()).isNotEmpty();
            assertThat(task.getJobId()).isNotEmpty();
            assertThat(task.getStatus().getReasonMessage()).isEmpty();
            assertThat(task.getStatusHistoryList()).isEmpty();
        }
    }

    @Test
    public void observeJobsWaitsForAllMarkers() {
        final List<Job> cellOneSnapshot = Arrays.asList(
                Job.newBuilder().setId("cell-1-job-1").setStatus(ACCEPTED_STATE).build(),
                Job.newBuilder().setId("cell-1-job-2").setStatus(ACCEPTED_STATE).build(),
                Job.newBuilder().setId("cell-1-job-3").setStatus(ACCEPTED_STATE).build()
        );
        final List<Job> cellTwoSnapshot = Arrays.asList(
                Job.newBuilder().setId("cell-2-job-1").setStatus(ACCEPTED_STATE).build(),
                Job.newBuilder().setId("cell-2-job-2").setStatus(ACCEPTED_STATE).build(),
                Job.newBuilder().setId("cell-2-job-3").setStatus(ACCEPTED_STATE).build()
        );

        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        final AssertableSubscriber<JobChangeNotification> testSubscriber = service.observeJobs(ObserveJobsQuery.getDefaultInstance()).test();
        List<JobChangeNotification> expected = Stream.concat(
                cellOneSnapshot.stream().map(this::toNotification).map(this::withStackName),
                cellTwoSnapshot.stream().map(this::toNotification).map(this::withStackName)
        ).collect(Collectors.toList());
        // single marker for all cells
        final JobChangeNotification mergedMarker = JobChangeNotification.newBuilder().setSnapshotEnd(SnapshotEnd.newBuilder()).build();
        expected.add(mergedMarker);

        testSubscriber.awaitValueCount(7, 1, TimeUnit.SECONDS);
        List<JobChangeNotification> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).last().isEqualTo(mergedMarker);
        assertThat(onNextEvents).containsExactlyInAnyOrder(expected.toArray(new JobChangeNotification[expected.size()]));

        // more updates are flowing
        final JobChangeNotification cellOneUpdate = toNotification(Job.newBuilder().setId("cell-1-job-10").setStatus(ACCEPTED_STATE).build());
        final JobChangeNotification cellTwoUpdate = toNotification(Job.newBuilder().setId("cell-2-job-10").setStatus(ACCEPTED_STATE).build());
        cellOneUpdates.onNext(cellOneUpdate);
        cellTwoUpdates.onNext(cellTwoUpdate);

        testSubscriber.awaitValueCount(9, 1, TimeUnit.SECONDS);
        onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).last().isNotEqualTo(mergedMarker);
        assertThat(onNextEvents).contains(withStackName(cellOneUpdate), withStackName(cellTwoUpdate));
    }

    @Test
    public void observeJobsStopsWhenAnyClientsTerminate() {
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellTwoUpdates.serialize()));

        final AssertableSubscriber<JobChangeNotification> testSubscriber = service.observeJobs(ObserveJobsQuery.getDefaultInstance()).test();

        final JobChangeNotification cellOneUpdate = toNotification(Job.newBuilder().setId("cell-1-job-100").setStatus(ACCEPTED_STATE).build());
        final JobChangeNotification cellTwoUpdate = toNotification(Job.newBuilder().setId("cell-2-job-200").setStatus(ACCEPTED_STATE).build());
        cellOneUpdates.onNext(cellOneUpdate);
        cellTwoUpdates.onNext(cellTwoUpdate);

        testSubscriber.awaitValueCount(2, 1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents()).isEmpty();
        assertThat(testSubscriber.isUnsubscribed()).isFalse();
        assertThat(testSubscriber.getCompletions()).isEqualTo(0);

        // a client finishes
        cellTwoUpdates.onCompleted();

        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents()).isEmpty();
        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(testSubscriber.getCompletions()).isEqualTo(1);
    }

    @Test
    public void observeJobsErrorsWhenAnyClientsError() {
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellTwoUpdates.serialize()));

        final AssertableSubscriber<JobChangeNotification> testSubscriber = service.observeJobs(ObserveJobsQuery.getDefaultInstance()).test();

        final JobChangeNotification cellOneUpdate = toNotification(Job.newBuilder().setId("cell-1-job-100").setStatus(ACCEPTED_STATE).build());
        final JobChangeNotification cellTwoUpdate = toNotification(Job.newBuilder().setId("cell-2-job-200").setStatus(ACCEPTED_STATE).build());
        cellOneUpdates.onNext(cellOneUpdate);
        cellTwoUpdates.onNext(cellTwoUpdate);

        testSubscriber.awaitValueCount(2, 1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents()).isEmpty();
        assertThat(testSubscriber.isUnsubscribed()).isFalse();
        assertThat(testSubscriber.getCompletions()).isEqualTo(0);

        // a client emits an error
        cellTwoUpdates.onError(new RuntimeException("unexpected error"));

        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents()).hasSize(1);
        assertThat(testSubscriber.isUnsubscribed()).isTrue();
        assertThat(testSubscriber.getCompletions()).isEqualTo(0);
    }

    @Test
    public void observeJob() {
        String cellOneJobId = UUID.randomUUID().toString();
        String cellTwoJobId = UUID.randomUUID().toString();
        cellOne.getServiceRegistry().addService(new CellWithJobStream(cellOneJobId, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithJobStream(cellTwoJobId, cellTwoUpdates.serialize()));

        AssertableSubscriber<JobChangeNotification> subscriber1 = service.observeJob(cellOneJobId).test();
        AssertableSubscriber<JobChangeNotification> subscriber2 = service.observeJob(cellTwoJobId).test();

        cellOneUpdates.onNext(toNotification(Job.newBuilder().setId(cellOneJobId).setStatus(ACCEPTED_STATE).build()));
        cellOneUpdates.onNext(toNotification(Job.newBuilder().setId(cellOneJobId).setStatus(KILL_INITIATED_STATE).build()));
        cellOneUpdates.onNext(toNotification(Job.newBuilder().setId(cellOneJobId).setStatus(FINISHED_STATE).build()));
        cellOneUpdates.onNext(toNotification(Job.newBuilder().setId(cellOneJobId).setStatus(ACCEPTED_STATE).build()));

        subscriber1.awaitValueCount(3, 5, TimeUnit.SECONDS);
        subscriber1.assertNoErrors();
        subscriber1.assertNotCompleted();
        assertThat(subscriber1.isUnsubscribed()).isFalse();

        subscriber2.assertNoErrors();
        subscriber2.assertNoValues();
        subscriber2.assertNotCompleted();

        cellTwoUpdates.onNext(toNotification(Task.newBuilder()
                .setId(cellTwoJobId + "-task1").setJobId(cellTwoJobId)
                .build())
        );
        subscriber2.awaitValueCount(1, 1, TimeUnit.SECONDS);
        subscriber2.assertNoErrors();
        subscriber2.assertNotCompleted();

        cellOneUpdates.onCompleted();

        subscriber1.awaitTerminalEvent(1, TimeUnit.SECONDS);
        assertThat(subscriber1.getOnErrorEvents()).isEmpty();
        assertThat(subscriber1.isUnsubscribed()).isTrue();
        assertThat(subscriber1.getCompletions()).isEqualTo(1);
    }

    @Test
    public void observeJobDoesNotSetDeadlines() throws InterruptedException {
        String cellOneJobId = UUID.randomUUID().toString();
        String cellTwoJobId = UUID.randomUUID().toString();
        cellOne.getServiceRegistry().addService(new CellWithJobStream(cellOneJobId, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithJobStream(cellTwoJobId, cellTwoUpdates.serialize()));

        List<AssertableSubscriber<JobChangeNotification>> subscribers = new LinkedList<>();
        subscribers.add(service.observeJob(cellOneJobId).test());
        subscribers.add(service.observeJob(cellTwoJobId).test());

        // TODO: make it easier to extract the Deadline for each cell call
        Thread.sleep(2 * GRPC_REQUEST_TIMEOUT_MS);

        for (AssertableSubscriber<JobChangeNotification> subscriber : subscribers) {
            subscriber.assertNoErrors();
            subscriber.assertNotCompleted();
            assertThat(subscriber.isUnsubscribed()).isFalse();
        }
    }

    @Test
    public void observeJobsDoesNotSetDeadlines() throws InterruptedException {
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellTwoUpdates.serialize()));

        AssertableSubscriber<JobChangeNotification> subscriber = service.observeJobs(ObserveJobsQuery.getDefaultInstance()).test();

        // TODO: make it easier to extract the Deadline for each cell call
        Thread.sleep(2 * GRPC_REQUEST_TIMEOUT_MS);

        subscriber.assertNoErrors();
        subscriber.assertNotCompleted();
        assertThat(subscriber.isUnsubscribed()).isFalse();
    }

    @Test
    public void createJobRouteToCorrectStack() {
        // Build service handlers for each cell
        cellToServiceMap.forEach((cell, grpcServerRule) ->
                grpcServerRule.getServiceRegistry().addService(new CellWithCachedJobsService(cell.getName()))
        );

        // Expected assignments based on routing rules in setUp()
        Map<String, String> expectedAssignmentMap = ImmutableMap.of(
                "app1", "one",
                "app2", "one",
                "app3", "two",
                "app4", "one"
        );

        expectedAssignmentMap.forEach((appName, expectedCellName) -> {
            // Create the job and let it get routed
            JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                    .setApplicationName(appName)
                    .setCapacityGroup(appName + "CapGroup")
                    .build();
            String jobId = service.createJob(jobDescriptor, JobManagerConstants.UNDEFINED_CALL_METADATA).toBlocking().first();

            // Get a client to the test gRPC service for the cell that we expect got it
            // TODO(Andrew L): This can use findJob() instead once AggregatingService implements it
            Cell expectedCell = getCellWithName(expectedCellName)
                    .orElseThrow(() -> TitusServiceException.cellNotFound(expectedCellName));
            JobManagementServiceStub expectedCellClient = JobManagementServiceGrpc.newStub(cellToServiceMap.get(expectedCell).getChannel());

            // Check that the cell has it with the correct attribute
            TestStreamObserver<Job> findJobResponse = new TestStreamObserver<>();
            expectedCellClient.findJob(JobId.newBuilder().setId(jobId).build(), findJobResponse);
            assertThatCode(() -> {
                Job job = findJobResponse.takeNext(1, TimeUnit.SECONDS);
                assertThat(job.getJobDescriptor().getAttributesOrThrow(JOB_ATTRIBUTES_CELL).equals(expectedCellName));
            }).doesNotThrowAnyException();
        });
    }

    @Test
    public void createJobInjectsFederatedStackName() {
        Cell firstCell = cells.get(0);
        CellWithCachedJobsService cachedJobsService = new CellWithCachedJobsService(firstCell.getName());
        cellToServiceMap.get(firstCell).getServiceRegistry().addService(cachedJobsService);

        // Create the job and let it get routed
        JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                .setApplicationName("app1")
                .setCapacityGroup("app1CapGroup")
                .build();
        String jobId = service.createJob(jobDescriptor, JobManagerConstants.UNDEFINED_CALL_METADATA).toBlocking().first();
        Optional<JobDescriptor> createdJob = cachedJobsService.getCachedJob(jobId);
        assertThat(createdJob).isPresent();
        assertThat(createdJob.get().getAttributesMap()).containsEntry(JOB_ATTRIBUTES_STACK, stackName);
    }

    private List<Job> walkAllFindJobsPages(int pageWalkSize) {
        return walkAllPages(
                pageWalkSize,
                service::findJobs,
                page -> JobQuery.newBuilder().setPage(page).build(),
                JobQueryResult::getPagination,
                JobQueryResult::getItemsList
        );
    }

    private List<Task> walkAllFindTasksPages(int pageWalkSize) {
        return walkAllPages(
                pageWalkSize,
                service::findTasks,
                page -> TaskQuery.newBuilder().setPage(page).build(),
                TaskQueryResult::getPagination,
                TaskQueryResult::getItemsList
        );
    }

    private JobChangeNotification toNotification(Job job) {
        return JobChangeNotification.newBuilder().setJobUpdate(JobUpdate.newBuilder().setJob(job)).build();
    }

    private JobChangeNotification toNotification(Task task) {
        return JobChangeNotification.newBuilder().setTaskUpdate(TaskUpdate.newBuilder().setTask(task)).build();
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

    private JobChangeNotification withStackName(JobChangeNotification jobChangeNotification) {
        switch (jobChangeNotification.getNotificationCase()) {
            case JOBUPDATE:
                JobUpdate jobUpdate = jobChangeNotification.getJobUpdate().toBuilder()
                        .setJob(withStackName(jobChangeNotification.getJobUpdate().getJob()))
                        .build();
                return jobChangeNotification.toBuilder().setJobUpdate(jobUpdate).build();
            default:
                return jobChangeNotification;
        }
    }

    private Optional<Cell> getCellWithName(String cellName) {
        return cellToServiceMap.keySet().stream().filter(cell -> cell.getName().equals(cellName)).findFirst();
    }

    private Pair<List<Job>, List<Job>> generateTestJobs() {
        final List<Job> cellOneSnapshot = new ArrayList<>();
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        // generate 5 groups of size 20 (10 service, 10 batch) with different timestamps
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                clock.advanceTime(1, TimeUnit.SECONDS);
                cellOneSnapshot.add(dataGenerator.newBatchJob(GrpcJobManagementModelConverters::toGrpcJob));

                clock.advanceTime(1, TimeUnit.SECONDS);
                cellTwoSnapshot.add(dataGenerator.newServiceJob(GrpcJobManagementModelConverters::toGrpcJob));

                clock.advanceTime(1, TimeUnit.SECONDS);
                cellOneSnapshot.add(dataGenerator.newServiceJob(GrpcJobManagementModelConverters::toGrpcJob));

                clock.advanceTime(1, TimeUnit.SECONDS);
                cellTwoSnapshot.add(dataGenerator.newBatchJob(GrpcJobManagementModelConverters::toGrpcJob));
            }
            clock.advanceTime(9, TimeUnit.SECONDS);
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));
        return Pair.of(cellOneSnapshot, cellTwoSnapshot);
    }

    private Pair<List<Task>, List<Task>> generateTestJobsWithTasks() {
        List<Task> cellOneSnapshot = new ArrayList<>();
        List<Task> cellTwoSnapshot = new ArrayList<>();

        // 10 jobs on each cell with TASKS_IN_GENERATED_JOBS tasks each
        for (int i = 0; i < 5; i++) {
            cellOneSnapshot.addAll(dataGenerator.newBatchJobWithTasks());
            cellTwoSnapshot.addAll(dataGenerator.newBatchJobWithTasks());
            cellOneSnapshot.addAll(dataGenerator.newServiceJobWithTasks());
            cellTwoSnapshot.addAll(dataGenerator.newServiceJobWithTasks());
            clock.advanceTime(1, TimeUnit.MINUTES);
        }

        cellOne.getServiceRegistry().addService(new CellWithFixedTasksService(cellOneSnapshot));
        cellTwo.getServiceRegistry().addService(new CellWithFixedTasksService(cellTwoSnapshot));
        return Pair.of(cellOneSnapshot, cellTwoSnapshot);
    }
}

