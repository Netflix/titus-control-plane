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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Empty;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.grpc.AnonymousSessionContext;
import com.netflix.titus.common.grpc.GrpcUtil;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.JobUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification.SnapshotEnd;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.observers.AssertableSubscriber;
import rx.subjects.PublishSubject;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingJobManagementServiceTest {
    private static final JobStatus ACCEPTED_STATE = JobStatus.newBuilder().setState(JobStatus.JobState.Accepted).build();

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellOneUpdates = PublishSubject.create();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellTwoUpdates = PublishSubject.create();

    private String stackName;
    private AggregatingJobManagementService service;
    private Map<Cell, GrpcServerRule> cellToServiceMap;
    private DataGenerator<Job> batchJobs;
    private DataGenerator<Job> serviceJobs;
    private DataGenerator<Task> batchTasks;
    private DataGenerator<Task> serviceTasks;
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

        service = new AggregatingJobManagementService(
                grpcClientConfiguration,
                titusFederationConfiguration,
                connector,
                cellRouter,
                new AnonymousSessionContext()
        );

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

        // expect stackName to have changed
        List<Job> expected = Stream.concat(cellOneSnapshot.stream(), cellTwoSnapshot.stream())
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
            batchJobs = batchJobs.apply(cellTwoSnapshot::add, 5);
            clock.advanceTime(1, TimeUnit.SECONDS);
            serviceJobs = serviceJobs.apply(cellTwoSnapshot::add, 5);
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

    @Test
    public void findTasksMergesAllCellsIntoSingleResult() {
        List<Task> cellOneSnapshot = new ArrayList<>();
        List<Task> cellTwoSnapshot = new ArrayList<>();
        DataGenerator<com.netflix.titus.api.jobmanager.model.job.Job<BatchJobExt>> myBatchJobs =
                JobGenerator.batchJobs(JobDescriptorGenerator.batchJobDescriptors().map(descriptor -> {
                    BatchJobExt extensions = descriptor.getExtensions().toBuilder()
                            .withSize(10)
                            .build();
                    return descriptor.toBuilder().withExtensions(extensions).build();
                }).getValue());
        DataGenerator<com.netflix.titus.api.jobmanager.model.job.Job<ServiceJobExt>> myServiceJobs =
                JobGenerator.serviceJobs(JobDescriptorGenerator.serviceJobDescriptors().map(descriptor -> {
                    ServiceJobExt extensions = descriptor.getExtensions().toBuilder()
                            .withCapacity(new Capacity(10, 10, 10))
                            .build();
                    return descriptor.toBuilder().withExtensions(extensions).build();
                }).getValue());

        // 10 jobs on each cell with 10 tasks each
        for (int i = 0; i < 5; i++) {
            myBatchJobs = generateBatchJobWithTasks(cellOneSnapshot::addAll, myBatchJobs);
            myBatchJobs = generateBatchJobWithTasks(cellTwoSnapshot::addAll, myBatchJobs);
            myServiceJobs = generateServiceJobWithTasks(cellOneSnapshot::addAll, myServiceJobs);
            myServiceJobs = generateServiceJobWithTasks(cellTwoSnapshot::addAll, myServiceJobs);
            clock.advanceTime(1, TimeUnit.MINUTES);
        }

        cellOne.getServiceRegistry().addService(new CellWithFixedTasksService(cellOneSnapshot));
        cellTwo.getServiceRegistry().addService(new CellWithFixedTasksService(cellTwoSnapshot));

        List<Task> tasks = walkAllFindTasksPages(7);
        assertThat(tasks).hasSize(cellOneSnapshot.size() + cellTwoSnapshot.size());
        List<Task> expected = Stream.concat(cellOneSnapshot.stream(), cellTwoSnapshot.stream())
                .sorted(JobManagerCursors.taskCursorOrderComparator())
                .map(this::withStackName)
                .collect(Collectors.toList());
        assertThat(tasks).containsExactlyElementsOf(expected);
    }

    private DataGenerator<com.netflix.titus.api.jobmanager.model.job.Job<BatchJobExt>> generateBatchJobWithTasks(Consumer<List<Task>> generatedTaskConsumer, DataGenerator<com.netflix.titus.api.jobmanager.model.job.Job<BatchJobExt>> jobs) {
        return jobs.apply(job -> {
            List<Task> tasks = JobGenerator.batchTasks(job)
                    .map(t -> V3GrpcModelConverters.toGrpcTask(t, new EmptyLogStorageInfo<>()))
                    .toList();
            generatedTaskConsumer.accept(tasks);
        }, 1);
    }

    private DataGenerator<com.netflix.titus.api.jobmanager.model.job.Job<ServiceJobExt>> generateServiceJobWithTasks(Consumer<List<Task>> generatedTaskConsumer, DataGenerator<com.netflix.titus.api.jobmanager.model.job.Job<ServiceJobExt>> jobs) {
        return jobs.apply(job -> {
            List<Task> tasks = JobGenerator.serviceTasks(job)
                    .map(t -> V3GrpcModelConverters.toGrpcTask(t, new EmptyLogStorageInfo<>()))
                    .toList();
            generatedTaskConsumer.accept(tasks);
        }, 1);
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

    /**
     * @param <Q> query type
     * @param <R> result type
     * @param <T> type of items in the result
     */
    private <Q, R, T> List<T> walkAllPages(int pageWalkSize,
                                           Function<Q, Observable<R>> pageFetcher,
                                           Function<com.netflix.titus.grpc.protogen.Page, Q> queryFactory,
                                           Function<R, com.netflix.titus.grpc.protogen.Pagination> paginationGetter,
                                           Function<R, List<T>> itemsGetter) {
        List<T> allItems = new ArrayList<>();
        Optional<R> lastResult = Optional.empty();
        int currentCursorPosition = -1;
        int currentPageNumber = 0;

        while (lastResult.map(r -> paginationGetter.apply(r).getHasMore()).orElse(true)) {
            com.netflix.titus.grpc.protogen.Page.Builder builder = com.netflix.titus.grpc.protogen.Page.newBuilder().setPageSize(pageWalkSize);
            if (lastResult.isPresent()) {
                builder.setCursor(paginationGetter.apply(lastResult.get()).getCursor());
            } else {
                builder.setPageNumber(0);
            }

            Q query = queryFactory.apply(builder.build());
            AssertableSubscriber<R> testSubscriber = pageFetcher.apply(query).test();
            testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
            testSubscriber.assertNoErrors().assertCompleted();
            testSubscriber.assertValueCount(1);

            final List<R> results = testSubscriber.getOnNextEvents();
            assertThat(results).hasSize(1);
            R result = results.get(0);
            List<T> items = itemsGetter.apply(result);
            com.netflix.titus.grpc.protogen.Pagination pagination = paginationGetter.apply(result);
            if (pagination.getHasMore()) {
                assertThat(items).hasSize(pageWalkSize);
            }
            currentCursorPosition += items.size();
            if (pagination.getTotalItems() > 0) {
                assertThat(pagination.getCursorPosition()).isEqualTo(currentCursorPosition);
            } else {
                assertThat(pagination.getCursorPosition()).isEqualTo(0);
            }
            assertThat(pagination.getCurrentPage().getPageNumber()).isEqualTo(currentPageNumber++);
            allItems.addAll(items);
            lastResult = Optional.of(result);
            testSubscriber.unsubscribe();
        }

        return allItems;
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
            String jobId = service.createJob(jobDescriptor).toBlocking().first();

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

    private JobChangeNotification toNotification(Job job) {
        return JobChangeNotification.newBuilder().setJobUpdate(JobUpdate.newBuilder().setJob(job)).build();
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

    private static class CellWithFixedTasksService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        private final List<Task> snapshot;

        private CellWithFixedTasksService(List<Task> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public void findTasks(TaskQuery request, StreamObserver<TaskQueryResult> responseObserver) {
            Pair<List<Task>, Pagination> page = PaginationUtil.takePageWithCursor(
                    toPage(request.getPage()),
                    snapshot,
                    JobManagerCursors.taskCursorOrderComparator(),
                    JobManagerCursors::taskIndexOf,
                    JobManagerCursors::newCursorFrom
            );
            TaskQueryResult result = TaskQueryResult.newBuilder()
                    .addAllItems(page.getLeft())
                    .setPagination(toGrpcPagination(page.getRight()))
                    .build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();
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

