package com.netflix.titus.runtime.endpoint.v3.grpc;

import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.ActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.service.JobActivityHistoryService;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;


import javax.inject.Inject;
import javax.inject.Singleton;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultJobActivityHistoryServiceGrpc extends JobActivityHistoryServiceGrpc.JobActivityHistoryServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJobActivityHistoryServiceGrpc.class);
    private final JobActivityHistoryService jobActivityHistoryService;
    CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultJobActivityHistoryServiceGrpc(JobActivityHistoryService jobActivityHistoryService,
                                                CallMetadataResolver callMetadataResolver) {
        this.jobActivityHistoryService = jobActivityHistoryService;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void viewScalingActivities(JobId request, StreamObserver<ActivityQueryResult> responseObserver) {
        logger.debug("Received view scaling activity request {}", request);
        Subscription subscription = jobActivityHistoryService.viewScalingActivities(request, resolveCallMetadata()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}
