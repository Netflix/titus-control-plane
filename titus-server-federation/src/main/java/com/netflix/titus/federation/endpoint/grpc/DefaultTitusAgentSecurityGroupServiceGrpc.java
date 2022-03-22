package com.netflix.titus.federation.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultJobActivityHistoryServiceGrpc;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupClient;
import io.reactivex.subscribers.DefaultSubscriber;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import reactor.core.CoreSubscriber;

import java.awt.event.ActionListener;
import java.util.function.Consumer;

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
                resetSecurityGroupResponse -> {
                    logger.debug(" Proxy server got response for " + request.getSecurityGroupID());
                    responseObserver.onNext(resetSecurityGroupResponse);
                    responseObserver.onCompleted();
                }, throwable -> {
                    logger.debug(" Proxy server got error for " + request.getSecurityGroupID());
                    responseObserver.onError(throwable);
                });
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}

