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

package io.netflix.titus.federation.service;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.JobUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification.SnapshotEnd;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatus;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.model.PaginationUtil;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.common.grpc.AnonymousSessionContext;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.time.TestClock;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.federation.startup.GrpcConfiguration;
import io.netflix.titus.federation.startup.TitusFederationConfiguration;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.netflix.titus.testkit.grpc.TestStreamObserver;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.observers.AssertableSubscriber;
import rx.subjects.PublishSubject;

import static io.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPage;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingJobManagementServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingJobManagementServiceTest.class);
    private static final JobStatus.Builder ACCEPTED_STATE = JobStatus.newBuilder().setState(JobStatus.JobState.Accepted);

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellOneUpdates = PublishSubject.create();


    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellTwoUpdates = PublishSubject.create();

    private String stackName;
    private AggregatingJobManagementService service;
    private Map<Cell, Pair<ManagedChannel, GrpcServerRule>> cellToChannelMap;
    private DataGenerator<Job> batchJobs;
    private DataGenerator<Job> serviceJobs;
    private TestClock clock;

    @Before
    public void setUp() {
        stackName = UUID.randomUUID().toString();

        GrpcConfiguration grpcClientConfiguration = mock(GrpcConfiguration.class);
        when(grpcClientConfiguration.getRequestTimeoutMs()).thenReturn(1000L);

        TitusFederationConfiguration titusFederationConfiguration = mock(TitusFederationConfiguration.class);
        when(titusFederationConfiguration.getStack()).thenReturn(stackName);
        when(titusFederationConfiguration.getCells()).thenReturn("one=1;two=2");
        when(titusFederationConfiguration.getRoutingRules()).thenReturn("one=(app1.*|app2.*);two=(app3.*)");

        CellInfoResolver cellInfoResolver = new DefaultCellInfoResolver(titusFederationConfiguration);
        DefaultCellRouter cellRouter = new DefaultCellRouter(cellInfoResolver, titusFederationConfiguration);
        List<Cell> cells = cellInfoResolver.resolve();
        cellToChannelMap = Collections.unmodifiableMap(Stream.of(
                new SimpleImmutableEntry<>(cells.get(0), Pair.of(cellOne.getChannel(), cellOne)),
                new SimpleImmutableEntry<>(cells.get(1), Pair.of(cellTwo.getChannel(), cellTwo)))
                .collect(Collectors.toMap(
                        SimpleImmutableEntry::getKey,
                        SimpleImmutableEntry::getValue,
                        (v1, v2) -> v2,
                        TreeMap::new)));

        CellConnector connector = mock(CellConnector.class);
        when(connector.getChannels()).thenReturn(cellToChannelMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, cellPairEntry -> cellPairEntry.getValue().getLeft())));
        Answer<Optional<ManagedChannel>> answer = (InvocationOnMock invocation) -> {
            Cell cell = invocation.getArgument(0);
            if (cellToChannelMap.containsKey(cell)) {
                return Optional.of(cellToChannelMap.get(cell).getLeft());
            }
            return Optional.empty();
        };
        when(connector.getChannelForCell(any(Cell.class)))
                .thenAnswer(answer);

        service = new AggregatingJobManagementService(grpcClientConfiguration, titusFederationConfiguration, connector, cellRouter, new AnonymousSessionContext());
        clock = Clocks.test();
        batchJobs = JobGenerator.batchJobs(JobDescriptorGenerator.batchJobDescriptors().getValue(), clock)
                .map(V3GrpcModelConverters::toGrpcJob);
        serviceJobs = JobGenerator.serviceJobs(JobDescriptorGenerator.serviceJobDescriptors().getValue(), clock)
                .map(V3GrpcModelConverters::toGrpcJob);
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
        for (int i = 0; i < 100; i++) {
            batchJobs = batchJobs.apply(cellOneSnapshot::add, random.nextInt(10));
            batchJobs = batchJobs.apply(cellTwoSnapshot::add, random.nextInt(10));
            serviceJobs = serviceJobs.apply(cellOneSnapshot::add, random.nextInt(10));
            serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, random.nextInt(10));
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
        assertThat(results.get(0).getItemsList()).hasSize(cellOneSnapshot.size() + cellTwoSnapshot.size());
    }

    @Test
    public void findJobsEmptyPage() {
        Random random = new Random();
        final List<Job> cellOneSnapshot = new ArrayList<>();
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            batchJobs = batchJobs.apply(cellOneSnapshot::add, random.nextInt(10));
            batchJobs = batchJobs.apply(cellTwoSnapshot::add, random.nextInt(10));
            serviceJobs = serviceJobs.apply(cellOneSnapshot::add, random.nextInt(10));
            serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, random.nextInt(10));
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
        final List<Job> cellOneSnapshot = new ArrayList<>();
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        // generate 5 groups of size 20 (10 service, 10 batch) with different timestamps
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                clock.advanceTime(1, TimeUnit.SECONDS);
                batchJobs = batchJobs.apply(cellOneSnapshot::add, 1);

                clock.advanceTime(1, TimeUnit.SECONDS);
                serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, 1);

                clock.advanceTime(1, TimeUnit.SECONDS);
                serviceJobs = serviceJobs.apply(cellOneSnapshot::add, 1);

                clock.advanceTime(1, TimeUnit.SECONDS);
                batchJobs = batchJobs.apply(cellTwoSnapshot::add, 1);
            }
            clock.advanceTime(9, TimeUnit.SECONDS);
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        List<Job> allJobs = walkAllFindJobsPages(10);
        assertThat(allJobs).hasSize(cellOneSnapshot.size() + cellTwoSnapshot.size());
        assertThat(allJobs).containsAll(cellOneSnapshot);
        assertThat(allJobs).containsAll(cellTwoSnapshot);
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
            batchJobs = batchJobs.apply(cellOneSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.SECONDS);
            serviceJobs = serviceJobs.apply(cellOneSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.SECONDS);
            batchJobs = batchJobs.apply(cellTwoSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.SECONDS);
            serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, 5);
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(cellOneSnapshot, cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        List<Job> allJobs = walkAllFindJobsPages(1);
        assertThat(allJobs).hasSize(cellOneSnapshot.size() + cellTwoSnapshot.size());
        assertThat(allJobs).containsAll(cellOneSnapshot);
        assertThat(allJobs).containsAll(cellTwoSnapshot);
    }

    /**
     * When one of the cells is empty, ensure that findJobs (including pagination) is working as if only one Cell
     * existed.
     */
    @Test
    public void findJobsWithEmptyCell() {
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batchJobs = batchJobs.apply(cellTwoSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.SECONDS);
            serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cellOne.getServiceRegistry().addService(new CellWithFixedJobsService(Collections.emptyList(), cellOneUpdates.serialize()));
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        List<Job> allJobs = walkAllFindJobsPages(5);
        assertThat(allJobs).hasSize(cellTwoSnapshot.size());
        assertThat(allJobs).containsAll(cellTwoSnapshot);
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
        final List<Job> cellTwoSnapshot = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batchJobs = batchJobs.apply(cellTwoSnapshot::add, 5);
            serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.MINUTES);
        }
        cellOne.getServiceRegistry().addService(new FailingCell());
        cellTwo.getServiceRegistry().addService(new CellWithFixedJobsService(cellTwoSnapshot, cellTwoUpdates.serialize()));

        JobQuery query = JobQuery.newBuilder()
                .setPage(toGrpcPage(Page.unlimited()))
                .build();

        final AssertableSubscriber<JobQueryResult> testSubscriber = service.findJobs(query).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertError(Status.INTERNAL.asRuntimeException().getClass());
        testSubscriber.assertNoValues();
    }

    private List<Job> walkAllFindJobsPages(int pageWalkSize) {
        List<Job> allJobs = new ArrayList<>();
        Optional<JobQueryResult> lastResult = Optional.empty();
        int currentCursorPosition = -1;
        int currentPageNumber = 0;

        while (lastResult.map(r -> r.getPagination().getHasMore()).orElse(true)) {
            final Page.Builder builder = Page.newBuilder().withPageSize(pageWalkSize);
            if (lastResult.isPresent()) {
                builder.withCursor(lastResult.get().getPagination().getCursor());
            } else {
                builder.withPageNumber(0);
            }

            final JobQuery query = JobQuery.newBuilder()
                    .setPage(toGrpcPage(builder.build()))
                    .build();
            final AssertableSubscriber<JobQueryResult> testSubscriber = service.findJobs(query).test();
            testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
            testSubscriber.assertNoErrors().assertCompleted();
            testSubscriber.assertValueCount(1);

            final List<JobQueryResult> results = testSubscriber.getOnNextEvents();
            assertThat(results).hasSize(1);
            JobQueryResult jobQueryResult = results.get(0);
            if (jobQueryResult.getPagination().getHasMore()) {
                assertThat(jobQueryResult.getItemsList()).hasSize(pageWalkSize);
            }
            currentCursorPosition += jobQueryResult.getItemsCount();
            if (jobQueryResult.getPagination().getTotalItems() > 0) {
                assertThat(jobQueryResult.getPagination().getCursorPosition()).isEqualTo(currentCursorPosition);
            } else {
                assertThat(jobQueryResult.getPagination().getCursorPosition()).isEqualTo(0);
            }
            assertThat(jobQueryResult.getPagination().getCurrentPage().getPageNumber()).isEqualTo(currentPageNumber++);
            allJobs.addAll(jobQueryResult.getItemsList());
            lastResult = Optional.of(jobQueryResult);
            testSubscriber.unsubscribe();
        }

        return allJobs;
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

        final AssertableSubscriber<JobChangeNotification> testSubscriber = service.observeJobs().test();
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

        final AssertableSubscriber<JobChangeNotification> testSubscriber = service.observeJobs().test();

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

        final AssertableSubscriber<JobChangeNotification> testSubscriber = service.observeJobs().test();

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
    public void createJobRouteToCorrectStack() throws InterruptedException {
        // Build service handlers for each cell
        cellToChannelMap.entrySet().forEach((cellPairEntry -> {
            Cell cell = cellPairEntry.getKey();
            GrpcServerRule grpcServerRule = cellPairEntry.getValue().getRight();
            grpcServerRule.getServiceRegistry().addService(new CellWithCachedJobsService(cell.getName()));
        }));

        // Expected assignments based on routing rules in setUp()
        Map<String, String> expectedAssignmentMap = Collections.unmodifiableMap(Stream.of(
                new SimpleImmutableEntry<>("app1", "one"),
                new SimpleImmutableEntry<>("app2", "one"),
                new SimpleImmutableEntry<>("app3", "two"),
                new SimpleImmutableEntry<>("app4", "one"))
                .collect(Collectors.toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue)));

        expectedAssignmentMap.forEach((appName, expectedCellName) -> {
            // Create the job and let it get routed
            JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                    .setApplicationName(appName)
                    .setCapacityGroup(appName + "CapGroup")
                    .build();
            String jobId = service.createJob(jobDescriptor).toBlocking().first();

            // Get a client to the test gRPC service for the cell that we expect got it
            // TODO(Andrew L): This can use findJob() instead once AggregatingService implements it
            Cell expectedCell = getCellWithName(expectedCellName).get();
            JobManagementServiceGrpc.JobManagementServiceStub expectedCellClient = JobManagementServiceGrpc.newStub(cellToChannelMap.get(expectedCell).getRight()
                    .getChannel());

            // Check that the cell has it with the correct attribute
            TestStreamObserver<Job> findJobResponse = new TestStreamObserver<>();
            expectedCellClient.findJob(JobId.newBuilder().setId(jobId).build(), findJobResponse);
            assertThatCode(() -> {
                Job job = findJobResponse.takeNext(1, TimeUnit.SECONDS);
                assertThat(job.getJobDescriptor().getAttributesOrThrow(JOB_ATTRIBUTES_CELL).equals(expectedCellName));
            }).doesNotThrowAnyException();
        });
    }

    private JobChangeNotification toNotification(Job job) {
        return JobChangeNotification.newBuilder().setJobUpdate(JobUpdate.newBuilder().setJob(job)).build();
    }

    private JobChangeNotification withStackName(JobChangeNotification jobChangeNotification) {
        switch (jobChangeNotification.getNotificationCase()) {
            case JOBUPDATE:
                JobDescriptor jobDescriptor = jobChangeNotification.getJobUpdate().getJob().getJobDescriptor().toBuilder()
                        .putAttributes("titus.stack", stackName)
                        .build();
                Job job = jobChangeNotification.getJobUpdate().getJob().toBuilder()
                        .setJobDescriptor(jobDescriptor)
                        .build();
                JobUpdate jobUpdate = jobChangeNotification.getJobUpdate().toBuilder()
                        .setJob(job)
                        .build();
                return jobChangeNotification.toBuilder().setJobUpdate(jobUpdate).build();
            default:
                return jobChangeNotification;
        }
    }

    private Optional<Cell> getCellWithName(String cellName) {
        return cellToChannelMap.keySet().stream().filter(cell -> cell.getName().equals(cellName)).findFirst();
    }

    private static class CellWithFixedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        private final List<Job> snapshot;
        private final Observable<JobChangeNotification> updates;

        private CellWithFixedJobsService(List<Job> snapshot, Observable<JobChangeNotification> updates) {
            this.snapshot = snapshot;
            this.updates = updates;
        }

        @Override
        public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
            Pair<List<Job>, Pagination> page = PaginationUtil.takePageWithCursor(
                    toPage(request.getPage()),
                    snapshot,
                    JobManagerCursors.jobCursorOrderComparator(),
                    JobManagerCursors::jobIndexOf,
                    JobManagerCursors::newCursorFrom
            );
            JobQueryResult result = JobQueryResult.newBuilder()
                    .addAllItems(page.getLeft())
                    .setPagination(toGrpcPagination(page.getRight()))
                    .build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        @Override
        public void observeJobs(Empty request, StreamObserver<JobChangeNotification> responseObserver) {
            for (Job job : snapshot) {
                JobUpdate update = JobUpdate.newBuilder().setJob(job).build();
                JobChangeNotification notification = JobChangeNotification.newBuilder().setJobUpdate(update).build();
                responseObserver.onNext(notification);
            }
            SnapshotEnd snapshotEnd = SnapshotEnd.newBuilder().build();
            JobChangeNotification marker = JobChangeNotification.newBuilder().setSnapshotEnd(snapshotEnd).build();
            responseObserver.onNext(marker);

            final Subscription subscription = updates.subscribe(
                    responseObserver::onNext,
                    responseObserver::onError,
                    responseObserver::onCompleted
            );
            GrpcUtil.attachCancellingCallback(responseObserver, subscription);
        }
    }

    private static class CellWithCachedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        private final String cellName;
        private final Map<JobId, JobDescriptor> jobDescriptorMap = new HashMap<>();

        private CellWithCachedJobsService(String name) {
            this.cellName = name;
        }

        @Override
        public void createJob(JobDescriptor request, StreamObserver<JobId> responseObserver) {
            JobId jobId = JobId.newBuilder().setId(UUID.randomUUID().toString()).build();
            jobDescriptorMap.put(
                    jobId,
                    JobDescriptor.newBuilder(request)
                            .putAttributes(JOB_ATTRIBUTES_CELL, cellName)
                            .build());
            final Subscription subscription = Observable.just(jobId)
                    .subscribe(
                            responseObserver::onNext,
                            responseObserver::onError,
                            responseObserver::onCompleted
                    );
            GrpcUtil.attachCancellingCallback(responseObserver, subscription);
        }

        @Override
        public void findJob(JobId request, StreamObserver<Job> responseObserver) {
            final Subscription subscription = Observable.just(Job.newBuilder().setJobDescriptor(jobDescriptorMap.get(request)).build())
                    .subscribe(
                            responseObserver::onNext,
                            responseObserver::onError,
                            responseObserver::onCompleted
                    );
            GrpcUtil.attachCancellingCallback(responseObserver, subscription);
        }
    }

    private static class FailingCell extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        @Override
        public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

}
