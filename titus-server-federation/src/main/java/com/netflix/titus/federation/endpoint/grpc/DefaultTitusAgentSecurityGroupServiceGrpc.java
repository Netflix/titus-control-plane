package com.netflix.titus.federation.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultJobActivityHistoryServiceGrpc;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupClient;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;

@Singleton
public class DefaultTitusAgentSecurityGroupServiceGrpc  extends TitusAgentSecurityGroupServiceGrpc.TitusAgentSecurityGroupServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJobActivityHistoryServiceGrpc.class);
    private final TitusAgentSecurityGroupClient titusAgentSecurityGroupServiceGateway;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultTitusAgentSecurityGroupServiceGrpc(TitusAgentSecurityGroupClient titusAgentSecurityGroupService, //client
                                                     CallMetadataResolver callMetadataResolver) {
        this.titusAgentSecurityGroupServiceGateway = titusAgentSecurityGroupService;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void resetSecurityGroup(com.netflix.titus.TitusVpcApi.ResetSecurityGroupRequest request,
                                   io.grpc.stub.StreamObserver<com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse> responseObserver) {

        titusAgentSecurityGroupServiceGateway.resetSecurityGroup(request, resolveCallMetadata()).subscribe(
                new Subscriber<ResetSecurityGroupResponse>() {
            @Override
            public void onSubscribe(Subscription s) {}

            @Override
            public void onNext(ResetSecurityGroupResponse resetSecurityGroupResponse) {
                responseObserver.onNext(resetSecurityGroupResponse);
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onComplete() {
                responseObserver.onCompleted();
            }
        });
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}

