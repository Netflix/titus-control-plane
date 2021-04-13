package com.netflix.titus.federation.service;

import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import rx.Observable;

import java.util.Optional;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static org.assertj.core.api.Assertions.assertThat;

class JobManagementServiceWithUnimplementedInterface extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    // All interface methods return UNIMPLEMENTED status.
}

class JobManagementServiceWithSlowMethods extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    public long createCount = 0;

    public void createJob(com.netflix.titus.grpc.protogen.JobDescriptor request,
                          io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.JobId> responseObserver) {
        // Increment call count, but never respond to the responseObserver
        createCount++;
    }
}

public class GrpcFallbackTest {
    private static final String appName = "app";
    private static final String cellName = "cell001";
    private static final long GRPC_REQUEST_TIMEOUT_MS = 1_000L;

    @Rule
    public GrpcServerRule federationRule = new GrpcServerRule().directExecutor();

    @Rule
    public GrpcServerRule cellRule = new GrpcServerRule().directExecutor();

    @BeforeEach
    void init() {
        federationRule = new GrpcServerRule().directExecutor();
        cellRule = new GrpcServerRule().directExecutor();
    }

    @Test
    public void unimplementedFallback() {
        CellWithCachedJobsService cachedJobsService = new CellWithCachedJobsService(cellName);
        cellRule.getServiceRegistry().addService(cachedJobsService);
        federationRule.getServiceRegistry().addService(new JobManagementServiceWithUnimplementedInterface());

        JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                .setApplicationName(appName)
                .build();
        Observable<String> fallbackObservbale = createJobWithFallback(jobDescriptor);

        String jobId = fallbackObservbale.toBlocking().first();
        Optional<JobDescriptor> createdJob = cachedJobsService.getCachedJob(jobId);
        assertThat(createdJob).isPresent();
    }

    @Test
    public void timeoutFallback() {
        CellWithCachedJobsService cachedJobsService = new CellWithCachedJobsService(cellName);
        JobManagementServiceWithSlowMethods slowJobsService = new JobManagementServiceWithSlowMethods();
        cellRule.getServiceRegistry().addService(cachedJobsService);
        federationRule.getServiceRegistry().addService(slowJobsService);

        JobDescriptor jobDescriptor = JobDescriptor.newBuilder()
                .setApplicationName(appName)
                .build();

        long initialCreatecount = slowJobsService.createCount;
        assertThat(initialCreatecount).isEqualTo(0);

        Observable<String> fallbackObservable = createJobWithFallback(jobDescriptor);
        String jobId = fallbackObservable.toBlocking().first();
        Optional<JobDescriptor> createdJob = cachedJobsService.getCachedJob(jobId);
        assertThat(createdJob).isPresent();

        assertThat(slowJobsService.createCount).isEqualTo(initialCreatecount + 1);
    }

    private Observable<String> createJobWithFallback(JobDescriptor jobDescriptor) {
        JobManagementServiceGrpc.JobManagementServiceStub fedClient = JobManagementServiceGrpc.newStub(federationRule.getChannel());
        JobManagementServiceGrpc.JobManagementServiceStub cellClient = JobManagementServiceGrpc.newStub(cellRule.getChannel());

        Observable<String> fedObservable = createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            fedClient.createJob(jobDescriptor, streamObserver);
        }, GRPC_REQUEST_TIMEOUT_MS);

        Observable<String> cellObservable = createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            cellClient.createJob(jobDescriptor, streamObserver);
        }, GRPC_REQUEST_TIMEOUT_MS);

        return fedObservable.onErrorResumeNext(cellObservable);
    }
}

