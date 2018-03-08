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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.JobUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification.SnapshotEnd;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobStatus;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.common.grpc.AnonymousSessionContext;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.util.CollectionsExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.observers.AssertableSubscriber;
import rx.subjects.PublishSubject;

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

    private CellConnector connector;
    private AggregatingJobManagementService service;

    @Before
    public void setUp() throws Exception {
        connector = mock(CellConnector.class);
        when(connector.getChannels()).thenReturn(CollectionsExt.<Cell, ManagedChannel>newHashMap()
                .entry(new Cell("one", "1"), cellOne.getChannel())
                .entry(new Cell("two", "2"), cellTwo.getChannel())
                .toMap()
        );
        service = new AggregatingJobManagementService(connector, new AnonymousSessionContext());
    }

    @After
    public void tearDown() throws Exception {
        cellOneUpdates.onCompleted();
        cellTwoUpdates.onCompleted();
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
                cellOneSnapshot.stream().map(this::toNotification),
                cellTwoSnapshot.stream().map(this::toNotification)
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
        assertThat(onNextEvents).contains(cellOneUpdate, cellTwoUpdate);
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

    private static class CellWithFixedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        private final List<Job> snapshot;
        private final Observable<JobChangeNotification> updates;

        private CellWithFixedJobsService(List<Job> snapshot, Observable<JobChangeNotification> updates) {
            this.snapshot = snapshot;
            this.updates = updates;
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