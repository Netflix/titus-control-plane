package com.netflix.titus.federation.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.ActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobActivityHistoryServiceGrpc;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.runtime.service.JobActivityHistoryService;
import io.grpc.stub.StreamObserver;
import rx.Observable;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.*;

@Singleton
public class DefaultJobActivityHistoryService implements JobActivityHistoryService {
    private final GrpcConfiguration grpcConfiguration;
    private final JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub client;
    @Inject
    public DefaultJobActivityHistoryService(GrpcConfiguration grpcConfiguration,
                                            JobActivityHistoryServiceGrpc.JobActivityHistoryServiceStub client) {
        this.grpcConfiguration = grpcConfiguration;
        this.client = client;
    }

    public Observable<ActivityQueryResult> viewScalingActivities(JobId jobId, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<ActivityQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, grpcConfiguration.getRequestTimeoutMs());
        }, grpcConfiguration.getRequestTimeoutMs());
    }
}
