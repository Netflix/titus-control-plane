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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatus;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.api.model.PaginationUtil;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.common.grpc.AnonymousSessionContext;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.federation.startup.GrpcConfiguration;
import io.netflix.titus.federation.startup.TitusFederationConfiguration;
import io.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.netflix.titus.runtime.jobmanager.JobManagerCursors;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.observers.AssertableSubscriber;
import rx.subjects.PublishSubject;

import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPage;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingJobManagementServiceTest {
    private static final JobStatus.Builder ACCEPTED_STATE = JobStatus.newBuilder().setState(JobStatus.JobState.Accepted);

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellOneUpdates = PublishSubject.create();


    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();
    private final PublishSubject<JobChangeNotification> cellTwoUpdates = PublishSubject.create();

    private String stackName;
    private AggregatingJobManagementService service;

    @Before
    public void setUp() throws Exception {
        stackName = UUID.randomUUID().toString();
        TitusFederationConfiguration configuration = mock(TitusFederationConfiguration.class);
        when(configuration.getStack()).thenReturn(stackName);
        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(1000L);
        CellConnector connector = mock(CellConnector.class);
        when(connector.getChannels()).thenReturn(CollectionsExt.<Cell, ManagedChannel>newHashMap()
                .entry(new Cell("one", "1"), cellOne.getChannel())
                .entry(new Cell("two", "2"), cellTwo.getChannel())
                .toMap()
        );
        service = new AggregatingJobManagementService(
                configuration, grpcConfiguration, connector, new AnonymousSessionContext()
        );
    }

    @After
    public void tearDown() throws Exception {
        cellOneUpdates.onCompleted();
        cellTwoUpdates.onCompleted();
    }

    @Test
    public void findJobsMergesAllCellsIntoSingleResult() {
        DataGenerator<Job> batchJobs = JobGenerator.batchJobs(JobDescriptorGenerator.batchJobDescriptors().getValue())
                .map(V3GrpcModelConverters::toGrpcJob);
        DataGenerator<Job> serviceJobs = JobGenerator.serviceJobs(JobDescriptorGenerator.serviceJobDescriptors().getValue())
                .map(V3GrpcModelConverters::toGrpcJob);

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
    public void findJobsPagination() {
        Assert.fail("TODO");
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

    private static class CellWithFixedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        private final List<Job> snapshot;
        private final Observable<JobChangeNotification> updates;

        private CellWithFixedJobsService(List<Job> snapshot, Observable<JobChangeNotification> updates) {
            this.snapshot = snapshot;
            this.updates = updates;
        }

        @Override
        public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
            final JobQueryResult.Builder builder = JobQueryResult.newBuilder();
            final Pair<List<Job>, Pagination> page = PaginationUtil.takePage(
                    toPage(request.getPage()), snapshot, JobManagerCursors::newCursorFrom
            );
            for (Job job : page.getLeft()) {
                builder.addItems(job);
            }
            builder.setPagination(toGrpcPagination(page.getRight()));
            responseObserver.onNext(builder.build());
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
}