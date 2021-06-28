package com.netflix.titus.federation.service;

import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

abstract class RemoteJobManagementService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    volatile public AtomicLong createCount = new AtomicLong(0);
    public UUID id = UUID.randomUUID();
}

class RemoteJobManagementServiceWithUnimplementedInterface extends RemoteJobManagementService {
    private static final Logger logger = LoggerFactory.getLogger(RemoteJobManagementServiceWithUnimplementedInterface.class);

    // All interface methods on super class return UNIMPLEMENTED status.
    public void createJob(com.netflix.titus.grpc.protogen.JobDescriptor request,
                          io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.JobId> responseObserver) {
        createCount.getAndIncrement();
        logger.info("id: {} createJob called {} time(s)", id, createCount);
        super.createJob(request, responseObserver);
    }
}

class RemoteJobManagementServiceWithUnavailableMethods extends RemoteJobManagementService {
    private static final Logger logger = LoggerFactory.getLogger(RemoteJobManagementServiceWithUnavailableMethods.class);

    public void createJob(com.netflix.titus.grpc.protogen.JobDescriptor request,
                          io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.JobId> responseObserver) {
        createCount.getAndIncrement();
        logger.info("id: {} createJob called {} time(s)", id, createCount);
        responseObserver.onError(Status.UNAVAILABLE
                .withDescription(String.format("Method %s is unavailable", "createJob"))
                .asRuntimeException());
    }
}

class RemoteJobManagementServiceWithTimeoutMethods extends RemoteJobManagementService {
    private static final Logger logger = LoggerFactory.getLogger(RemoteJobManagementServiceWithUnavailableMethods.class);

    public void createJob(com.netflix.titus.grpc.protogen.JobDescriptor request,
                          io.grpc.stub.StreamObserver<com.netflix.titus.grpc.protogen.JobId> responseObserver) {
        // Never respond to force timeout
        createCount.getAndIncrement();
        logger.info("id: {} createJob called {} time(s)", id, createCount);
    }
}
