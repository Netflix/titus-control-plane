package com.netflix.titus.runtime.endpoint.v3.grpc;

import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.ResetSecurityGroupResponse;
import com.netflix.titus.grpc.protogen.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.grpc.protogen.ResetSecurityGroupRequest;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;


import javax.inject.Inject;
import javax.inject.Singleton;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class DefaultTitusAgentSecurityGroupServiceGrpc  extends TitusAgentSecurityGroupServiceGrpc.TitusAgentSecurityGroupServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJobActivityHistoryServiceGrpc.class);
    private final TitusAgentSecurityGroupService titusAgentSecurityGroupService;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultTitusAgentSecurityGroupServiceGrpc(TitusAgentSecurityGroupService titusAgentSecurityGroupService,
                                                CallMetadataResolver callMetadataResolver) {
        this.titusAgentSecurityGroupService = titusAgentSecurityGroupService;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void ResetSecurityGroup(ResetSecurityGroupRequest request, ResetSecurityGroupResponse responseObserver) {
        logger.debug("Received ResetSecurityGroup request {}", request);
        return titusAgentSecurityGroupService.ResetSecurityGroup(request);
    }

    private CallMetadata resolveCallMetadata() {
        return callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);
    }
}

