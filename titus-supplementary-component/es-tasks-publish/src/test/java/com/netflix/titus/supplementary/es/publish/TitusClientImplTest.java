package com.netflix.titus.supplementary.es.publish;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.fail;

public class TitusClientImplTest {

    private static TitusClient titusClient;
    private static Server testServer;

    private static BatchJobTask taskOne = JobGenerator.oneBatchTask();
    private static BatchJobTask taskTwo = JobGenerator.oneBatchTask();
    private static BatchJobTask taskThree = JobGenerator.oneBatchTask();
    private static BatchJobTask taskFour = JobGenerator.oneBatchTask();
    private static BatchJobTask taskFive = JobGenerator.oneBatchTask();
    private static Job<BatchJobExt> jobOne = JobGenerator.oneBatchJob();

    public static class MockJobManagerService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
        @Override
        public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
            final Task grpcTask = V3GrpcModelConverters.toGrpcTask(taskOne, new EmptyLogStorageInfo<>());
            responseObserver.onNext(grpcTask);
            responseObserver.onCompleted();
        }

        @Override
        public void findJob(JobId request, StreamObserver<com.netflix.titus.grpc.protogen.Job> responseObserver) {
            final com.netflix.titus.grpc.protogen.Job grpcJob = V3GrpcModelConverters.toGrpcJob(jobOne);
            responseObserver.onNext(grpcJob);
            responseObserver.onCompleted();
        }

        @Override
        public void observeJobs(ObserveJobsQuery request, StreamObserver<JobChangeNotification> responseObserver) {
            final List<BatchJobTask> batchJobTasks = Arrays.asList(taskOne, taskTwo, taskThree, taskFour, taskFive);
            batchJobTasks.forEach(task -> {
                final Task grpcTask = V3GrpcModelConverters.toGrpcTask(task, new EmptyLogStorageInfo<>());
                responseObserver.onNext(buildJobChangeNotification(grpcTask));
            });
            responseObserver.onCompleted();
        }

        private JobChangeNotification buildJobChangeNotification(Task task) {
            return JobChangeNotification.newBuilder()
                    .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(task).build())
                    .build();
        }
    }

    @Before
    public void setup() throws IOException {
        final MockJobManagerService mockJobManagerService = new MockJobManagerService();

        testServer = InProcessServerBuilder
                .forName("testServer")
                .directExecutor()
                .addService(mockJobManagerService)
                .build()
                .start();

        final ManagedChannel channel = InProcessChannelBuilder
                .forName("testServer")
                .directExecutor()
                .usePlaintext(true)
                .build();
        final JobManagementServiceStub jobManagementServiceStub = JobManagementServiceGrpc.newStub(channel);
        titusClient = new TitusClientImpl(jobManagementServiceStub, new DefaultRegistry());
    }

    @After
    public void cleanup() {
        testServer.shutdownNow();
    }

    @Test
    public void getTaskById() {
        final CountDownLatch latch = new CountDownLatch(1);
        titusClient.getTask(taskOne.getId()).subscribe(task -> {
            assertThat(task.getId()).isEqualTo(taskOne.getId());
            assertThat(task.getJobId()).isEqualTo(taskOne.getJobId());
            latch.countDown();
        });
        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getTaskById Timeout ", e);
        }
    }

    @Test
    public void getJobById() {
        final CountDownLatch latch = new CountDownLatch(1);
        titusClient.getJobById(jobOne.getId()).subscribe(job -> {
            assertThat(job.getId()).isEqualTo(jobOne.getId());
            latch.countDown();
        });
        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getJobById Timeout ", e);
        }
    }

    @Test
    public void getTaskUpdates() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger tasksCount = new AtomicInteger(0);
        titusClient.getTaskUpdates().subscribe(task -> {
            if (tasksCount.incrementAndGet() == 5) {
                latch.countDown();
            }
        }, e -> fail("getTaskUpdates exception {}", e));
        try {
            latch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("getTaskUpdates Timeout ", e);
        }
    }
}

