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

package com.netflix.titus.runtime.connector.jobmanager;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.FunctionExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.KeepAliveRequest;
import com.netflix.titus.grpc.protogen.KeepAliveResponse;
import com.netflix.titus.grpc.protogen.ObserveJobsWithKeepAliveRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class RemoteJobManagementClientWithKeepAliveTest {

    private static final Object COMPLETED = new Object();

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobConnectorConfiguration configuration = Archaius2Ext.newConfiguration(JobConnectorConfiguration.class);

    private final ReactorJobManagementServiceStub reactorStub = mock(ReactorJobManagementServiceStub.class);

    private RemoteJobManagementClientWithKeepAlive client;
    private Server server;
    private ManagedChannel channel;

    private ServerCallStreamObserver<JobChangeNotification> responseObserver;

    private final StreamObserver<ObserveJobsWithKeepAliveRequest> requestObserver = new StreamObserver<ObserveJobsWithKeepAliveRequest>() {
        @Override
        public void onNext(ObserveJobsWithKeepAliveRequest value) {
            receivedFromClient.add(value);
        }

        @Override
        public void onError(Throwable error) {
            receivedFromClient.add(error);
        }

        @Override
        public void onCompleted() {
            receivedFromClient.add(COMPLETED);
        }
    };

    private final BlockingQueue<Object> receivedFromClient = new LinkedBlockingDeque<>();
    private boolean clientCancelled;

    @Before
    public void setUp() {
        this.server = newServerConnection();
        this.channel = InProcessChannelBuilder.forName("test").directExecutor().build();
        this.client = new RemoteJobManagementClientWithKeepAlive(
                "test",
                configuration,
                JobManagementServiceGrpc.newStub(channel),
                reactorStub,
                titusRuntime
        );
    }

    @After
    public void tearDown() {
        Evaluators.acceptNotNull(channel, ManagedChannel::shutdown);
        Evaluators.acceptNotNull(server, Server::shutdown);
    }

    @Test
    public void testJobEvent() throws InterruptedException {
        Iterator<JobChangeNotification> it = newClientConnection();
        responseObserver.onNext(newJobUpdateEvent("job1"));
        expectJobChangeNotification(it, JobChangeNotification.NotificationCase.JOBUPDATE);
    }

    @Test
    public void testKeepAlive() throws InterruptedException {
        Iterator<JobChangeNotification> it = newClientConnection();
        KeepAliveRequest keepAliveRequest = waitForClientKeepAliveRequest();
        responseObserver.onNext(JobChangeNotification.newBuilder()
                .setKeepAliveResponse(KeepAliveResponse.newBuilder().setRequest(keepAliveRequest).build())
                .build()
        );
        KeepAliveResponse keepAliveResponse = expectJobChangeNotification(it,
                JobChangeNotification.NotificationCase.KEEPALIVERESPONSE).getKeepAliveResponse();
        assertThat(keepAliveResponse.getRequest()).isEqualTo(keepAliveRequest);
    }

    @Test
    public void testClientCancel() throws InterruptedException {
        AtomicBoolean keepAliveCompleted = new AtomicBoolean();
        Disposable disposable = client.connectObserveJobs(Collections.emptyMap(), () -> keepAliveCompleted.set(true)).subscribe();
        // Read and discard the query message
        receivedFromClient.poll(30, TimeUnit.SECONDS);
        disposable.dispose();
        Object value = receivedFromClient.poll(30, TimeUnit.SECONDS);
        assertThat(value).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) value).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
        assertThat(keepAliveCompleted).isTrue();
        assertThat(clientCancelled).isTrue();
    }

    @Test
    public void testServerError() throws InterruptedException {
        AtomicBoolean keepAliveCompleted = new AtomicBoolean();
        Iterator<JobChangeNotification> it = newClientConnection(() -> keepAliveCompleted.set(true));
        // Read and discard the query message
        receivedFromClient.poll(30, TimeUnit.SECONDS);

        responseObserver.onError(new StatusRuntimeException(Status.ABORTED.augmentDescription("simulated error")));
        try {
            it.next();
            fail("expected an exception");
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
            assertThat(keepAliveCompleted).isTrue();
        }
    }

    @Test
    public void testServerCompleted() throws InterruptedException {
        AtomicBoolean keepAliveCompleted = new AtomicBoolean();
        Iterator<JobChangeNotification> it = newClientConnection(() -> keepAliveCompleted.set(true));
        waitForClientKeepAliveRequest();

        responseObserver.onCompleted();
        assertThat(it.hasNext()).isFalse();
        assertThat(keepAliveCompleted).isTrue();
    }

    private Server newServerConnection() {
        try {
            return InProcessServerBuilder.forName("test")
                    .directExecutor()
                    .addService(new GrpcJobServiceStub())
                    .build()
                    .start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Iterator<JobChangeNotification> newClientConnection(Runnable keepAliveCompleted) throws InterruptedException {
        Iterator<JobChangeNotification> it = client.connectObserveJobs(Collections.emptyMap(), keepAliveCompleted).toIterable().iterator();

        Object clientRequestEvent = receivedFromClient.poll(30, TimeUnit.SECONDS);
        assertThat(clientRequestEvent).isNotNull().isInstanceOf(ObserveJobsWithKeepAliveRequest.class);
        assertThat(((ObserveJobsWithKeepAliveRequest) clientRequestEvent).getKindCase()).isEqualTo(ObserveJobsWithKeepAliveRequest.KindCase.QUERY);
        return it;
    }

    private Iterator<JobChangeNotification> newClientConnection() throws InterruptedException {
        return newClientConnection(FunctionExt.noop());
    }

    private JobChangeNotification expectJobChangeNotification(Iterator<JobChangeNotification> it, JobChangeNotification.NotificationCase eventCase) {
        JobChangeNotification jobEvent = it.next();
        assertThat(jobEvent).isNotNull();
        assertThat(jobEvent.getNotificationCase()).isEqualTo(eventCase);
        return jobEvent;
    }

    private KeepAliveRequest waitForClientKeepAliveRequest() throws InterruptedException {
        Object value = receivedFromClient.poll(30, TimeUnit.SECONDS);
        assertThat(value).isNotNull().isInstanceOf(ObserveJobsWithKeepAliveRequest.class);
        ObserveJobsWithKeepAliveRequest event = (ObserveJobsWithKeepAliveRequest) value;
        assertThat(event.getKindCase()).isEqualTo(ObserveJobsWithKeepAliveRequest.KindCase.KEEPALIVEREQUEST);
        return event.getKeepAliveRequest();
    }

    private JobChangeNotification newJobUpdateEvent(String jobId) {
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder()
                        .setJob(Job.newBuilder()
                                .setId(jobId)
                                .build()
                        )
                        .build()
                )
                .build();
    }

    private class GrpcJobServiceStub extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        @Override
        public StreamObserver<ObserveJobsWithKeepAliveRequest> observeJobsWithKeepAlive(StreamObserver<JobChangeNotification> responseObserver) {
            ServerCallStreamObserver<JobChangeNotification> responseObserver2 = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
            RemoteJobManagementClientWithKeepAliveTest.this.responseObserver = responseObserver2;
            responseObserver2.setOnCancelHandler(() -> RemoteJobManagementClientWithKeepAliveTest.this.clientCancelled = true);
            return requestObserver;
        }
    }
}