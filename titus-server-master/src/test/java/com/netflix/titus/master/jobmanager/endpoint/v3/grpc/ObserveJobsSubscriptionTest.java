/*
 * Copyright 2021 Netflix, Inc.
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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobChangeNotification.JobUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification.TaskUpdate;
import com.netflix.titus.grpc.protogen.KeepAliveRequest;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.ObserveJobsWithKeepAliveRequest;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.NoOpGrpcObjectsCache;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class ObserveJobsSubscriptionTest {

    private static final String SERVICE_JOB_WITH_ONE_TASK = "serviceJobWithOneTask";

    private static final ObserveJobsQuery QUERY = ObserveJobsQuery.newBuilder().putFilteringCriteria("jobType", "service").build();

    private static final ObserveJobsWithKeepAliveRequest QUERY_REQUEST = ObserveJobsWithKeepAliveRequest.newBuilder()
            .setQuery(QUERY)
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestScheduler testScheduler = Schedulers.test();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final ObserveJobsContext context = new ObserveJobsContext(
            jobComponentStub.getJobOperations(),
            AnonymousCallMetadataResolver.getInstance(),
            new NoOpGrpcObjectsCache(),
            testScheduler,
            new DefaultJobManagementServiceGrpcMetrics(titusRuntime),
            titusRuntime
    );

    private final ObserveJobsSubscription jobsSubscription = new ObserveJobsSubscription(context);

    private StreamObserver<JobChangeNotification> responseStreamObserver;
    private final BlockingQueue<JobChangeNotification> responseEvents = new LinkedBlockingDeque<>();
    private Throwable responseError;
    private boolean responseCompleted;

    @Before
    public void setUp() throws Exception {
        jobComponentStub.addJobTemplate(SERVICE_JOB_WITH_ONE_TASK,
                JobDescriptorGenerator.serviceJobDescriptors()
                        .map(jd -> JobFunctions.changeServiceJobCapacity(jd, 1))
                        .cast(JobDescriptor.class)
        );

        this.responseStreamObserver = new ServerCallStreamObserver<JobChangeNotification>() {
            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public void setOnCancelHandler(Runnable onCancelHandler) {
            }

            @Override
            public void setCompression(String compression) {
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setOnReadyHandler(Runnable onReadyHandler) {
            }

            @Override
            public void disableAutoInboundFlowControl() {
            }

            @Override
            public void request(int count) {
            }

            @Override
            public void setMessageCompression(boolean enable) {
            }

            @Override
            public void onNext(JobChangeNotification event) {
                responseEvents.add(event);
            }

            @Override
            public void onError(Throwable error) {
                responseError = error;
            }

            @Override
            public void onCompleted() {
                responseCompleted = true;
            }
        };
    }

    @Test
    public void testObserveJobsSnapshot() {
        Job<?> job1 = jobComponentStub.createJob(SERVICE_JOB_WITH_ONE_TASK);
        Task task1 = jobComponentStub.createDesiredTasks(job1).get(0);

        // First snapshot
        jobsSubscription.observeJobs(QUERY, responseStreamObserver);
        assertThat(expectJobUpdateEvent().getJob().getId()).isEqualTo(job1.getId());
        assertThat(expectTaskUpdateEvent().getTask().getId()).isEqualTo(task1.getId());
        expectSnapshotEvent();
        triggerActions(1);

        // Now changes
        Job<?> job2 = jobComponentStub.createJob(SERVICE_JOB_WITH_ONE_TASK);
        triggerActions(1);
        assertThat(expectJobUpdateEvent().getJob().getId()).isEqualTo(job2.getId());
    }

    @Test
    public void testObserveJobsWithKeepAliveSnapshot() {
        Job<?> job1 = jobComponentStub.createJob(SERVICE_JOB_WITH_ONE_TASK);
        Task task1 = jobComponentStub.createDesiredTasks(job1).get(0);

        StreamObserver<ObserveJobsWithKeepAliveRequest> request = jobsSubscription.observeJobsWithKeepAlive(responseStreamObserver);

        // Check that nothing is emitted until we send the query request
        request.onNext(newKeepAliveRequest(1));
        triggerActions(5);
        assertThat(responseEvents.poll()).isNull();

        // Now send the request and read the snapshot
        request.onNext(QUERY_REQUEST);
        request.onNext(newKeepAliveRequest(2));

        assertThat(expectJobUpdateEvent().getJob().getId()).isEqualTo(job1.getId());
        assertThat(expectTaskUpdateEvent().getTask().getId()).isEqualTo(task1.getId());
        expectSnapshotEvent();
        triggerActions(1);

        jobComponentStub.emitCheckpoint();
        triggerActions(1);
        expectKeepAlive(2);
        triggerActions(1);

        // Now changes
        Job<?> job2 = jobComponentStub.createJob(SERVICE_JOB_WITH_ONE_TASK);
        triggerActions(1);
        assertThat(expectJobUpdateEvent().getJob().getId()).isEqualTo(job2.getId());

        // Now keep alive
        request.onNext(newKeepAliveRequest(3));
        jobComponentStub.emitCheckpoint();
        triggerActions(1);
        expectKeepAlive(3);
    }

    @Test
    public void testClientRequestError() {
        StreamObserver<ObserveJobsWithKeepAliveRequest> request = jobsSubscription.observeJobsWithKeepAlive(responseStreamObserver);
        request.onNext(QUERY_REQUEST);

        assertThat(jobsSubscription.jobServiceSubscription.isUnsubscribed()).isFalse();

        request.onError(new RuntimeException("simulated client error"));
        jobComponentStub.createJob(SERVICE_JOB_WITH_ONE_TASK);

        // Check that job service event stream subscription is closed.
        triggerActions(5);
        assertThat(jobsSubscription.jobServiceSubscription.isUnsubscribed()).isTrue();
    }

    @Test
    public void testKeepAlive() {
        StreamObserver<ObserveJobsWithKeepAliveRequest> request = jobsSubscription.observeJobsWithKeepAlive(responseStreamObserver);
        // Query first
        request.onNext(QUERY_REQUEST);
        expectSnapshotEvent();

        // Now keep alive
        request.onNext(newKeepAliveRequest(123));
        triggerActions(5);
        JobChangeNotification nextEvent = responseEvents.poll();
        assertThat(nextEvent).isNull();

        jobComponentStub.emitCheckpoint();
        triggerActions(5);
        nextEvent = responseEvents.poll();
        assertThat(nextEvent).isNotNull();
        assertThat(nextEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.KEEPALIVERESPONSE);
        assertThat(nextEvent.getKeepAliveResponse().getRequest().getRequestId()).isEqualTo(123);
    }

    private ObserveJobsWithKeepAliveRequest newKeepAliveRequest(long id) {
        return ObserveJobsWithKeepAliveRequest.newBuilder().setKeepAliveRequest(
                KeepAliveRequest.newBuilder().setRequestId(id).build()
        ).build();
    }

    private void triggerActions(int count) {
        for (int i = 0; i < count; i++) {
            testScheduler.triggerActions();
        }
    }

    private JobUpdate expectJobUpdateEvent() {
        JobChangeNotification nextEvent = responseEvents.poll();
        assertThat(nextEvent).isNotNull();
        assertThat(nextEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.JOBUPDATE);
        return nextEvent.getJobUpdate();
    }

    private TaskUpdate expectTaskUpdateEvent() {
        JobChangeNotification nextEvent = responseEvents.poll();
        assertThat(nextEvent).isNotNull();
        assertThat(nextEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.TASKUPDATE);
        return nextEvent.getTaskUpdate();
    }

    private void expectSnapshotEvent() {
        JobChangeNotification nextEvent = responseEvents.poll();
        assertThat(nextEvent).isNotNull();
        assertThat(nextEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.SNAPSHOTEND);
    }

    private void expectKeepAlive(int keepAliveRequestId) {
        JobChangeNotification nextEvent = responseEvents.poll();
        assertThat(nextEvent).isNotNull();
        assertThat(nextEvent.getNotificationCase()).isEqualTo(JobChangeNotification.NotificationCase.KEEPALIVERESPONSE);
        assertThat(nextEvent.getKeepAliveResponse().getRequest().getRequestId()).isEqualTo(keepAliveRequestId);

    }
}