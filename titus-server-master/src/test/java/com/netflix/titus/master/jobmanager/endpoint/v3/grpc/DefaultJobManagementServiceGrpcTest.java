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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.util.Optional;

import com.google.protobuf.Empty;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.GrpcTitusServiceGateway;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.subjects.PublishSubject;

import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toGrpcJob;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultJobManagementServiceGrpcTest {

    private static final CallMetadata CALL_METADATA = CallMetadata.newBuilder().withCallerId("unitTest").build();

    private static Job JOB = toGrpcJob(JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue());
    private static JobDescriptor JOB_DESCRIPTOR = JOB.getJobDescriptor();

    private final GrpcTitusServiceGateway gateway = mock(GrpcTitusServiceGateway.class);

    private final CallMetadataResolver callMetadataResolver = mock(CallMetadataResolver.class);

    private final DefaultJobManagementServiceGrpc service = new DefaultJobManagementServiceGrpc(gateway, callMetadataResolver);

    @Before
    public void setUp() throws Exception {
        when(callMetadataResolver.resolve()).thenReturn(Optional.of(CALL_METADATA));
    }

    @Test
    public void testCreateJob() throws Exception {
        when(gateway.createJob(JOB_DESCRIPTOR)).thenReturn(Observable.just(JOB.getId()));

        TestStreamObserver<JobId> response = new TestStreamObserver<>();
        service.createJob(JOB_DESCRIPTOR, response);

        assertSucceededWith(response, JobId.newBuilder().setId(JOB.getId()).build());
    }

    @Test
    public void testUpdateJobInstances() throws Exception {
        when(gateway.resizeJob(any(), any(), anyInt(), anyInt(), anyInt())).thenReturn(Observable.empty());

        TestStreamObserver<Empty> response = new TestStreamObserver<>();
        JobCapacityUpdate update = JobCapacityUpdate.newBuilder()
                .setJobId(JOB.getId())
                .setCapacity(
                        Capacity.newBuilder().setDesired(1).setMin(2).setMax(3)
                ).build();
        service.updateJobCapacity(update, response);

        verify(gateway, times(1)).resizeJob("calledBy=unitTest, relayedVia=direct to TitusMaster", JOB.getId(), 1, 2, 3);
    }

    @Test
    public void testUpdateJobProcesses() throws Exception {
        when(gateway.updateJobProcesses(any(), any(), anyBoolean(), anyBoolean())).thenReturn(Observable.empty());
        TestStreamObserver<Empty> response = new TestStreamObserver<>();
        JobProcessesUpdate jobProcessesUpdate = JobProcessesUpdate.newBuilder().setJobId(JOB.getId()).setServiceJobProcesses(
                ServiceJobSpec.ServiceJobProcesses.newBuilder().setDisableDecreaseDesired(true).setDisableIncreaseDesired(false).build()
        ).build();
        service.updateJobProcesses(jobProcessesUpdate, response);
        verify(gateway, times(1)).updateJobProcesses("calledBy=unitTest, relayedVia=direct to TitusMaster", JOB.getId(),
                true, false);
    }

    @Test
    public void testFindAllJobs() throws Exception {
        Page requestedPage = Page.newBuilder().setPageNumber(1).setPageSize(2).build();
        JobQuery jobQuery = JobQuery.newBuilder().setPage(requestedPage).build();

        com.netflix.titus.api.model.Pagination pagination = new com.netflix.titus.api.model.Pagination(
                toPage(requestedPage), false, 1, 1, "", 0
        );
        when(gateway.findJobsByCriteria(any(), any())).thenReturn(Pair.of(singletonList(JOB), pagination));

        TestStreamObserver<JobQueryResult> response = new TestStreamObserver<>();
        service.findJobs(jobQuery, response);

        JobQueryResult expectedResponse = JobQueryResult.newBuilder()
                .addItems(JOB)
                .setPagination(Pagination.newBuilder().setCurrentPage(requestedPage).setTotalItems(1).setTotalPages(1))
                .build();
        assertSucceededWith(response, expectedResponse);
    }

    @Test
    public void testObserveJobs() throws Exception {
        PublishSubject<JobChangeNotification> eventSubject = PublishSubject.create();
        when(gateway.observeJobs()).thenReturn(eventSubject);

        TestStreamObserver<JobChangeNotification> response = new TestStreamObserver<>();
        service.observeJobs(Empty.getDefaultInstance(), response);

        emitJobUpdateAndDisconnect(eventSubject, response);
    }

    @Test
    public void testObserveJob() throws Exception {
        PublishSubject<JobChangeNotification> eventSubject = PublishSubject.create();
        when(gateway.observeJob(JOB.getId())).thenReturn(eventSubject);

        TestStreamObserver<JobChangeNotification> response = new TestStreamObserver<>();
        service.observeJob(JobId.newBuilder().setId(JOB.getId()).build(), response);

        emitJobUpdateAndDisconnect(eventSubject, response);
    }

    private void emitJobUpdateAndDisconnect(PublishSubject<JobChangeNotification> eventSubject, TestStreamObserver<JobChangeNotification> response) {
        eventSubject.onNext(createJobFinishedUpdate());
        assertThat(response.takeNext().getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.JOBUPDATE);

        assertThat(eventSubject.hasObservers()).isTrue();
        response.cancel();
        assertThat(eventSubject.hasObservers()).isFalse();
    }

    private JobChangeNotification createJobFinishedUpdate() {
        return JobChangeNotification.newBuilder().setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(JOB)).build();
    }

    private <T> void assertSucceededWith(TestStreamObserver<T> response, T expectedValue) {
        if (response.hasError()) {
            throw new IllegalStateException(response.getError());
        }
        assertThat(response.isCompleted()).describedAs("Stream should be terminated").isTrue();
        assertThat(response.takeNext()).isEqualTo(expectedValue);
    }
}