package com.netflix.titus.federation.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupService;
import com.netflix.titus.grpc.protogen.TitusAgentSecurityGroupServiceGrpc.TitusAgentSecurityGroupServiceStub;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DefaultTitusAgentSecurityGroupService implements TitusAgentSecurityGroupService {

    private final GrpcConfiguration grpcConfiguration;
    private TitusAgentSecurityGroupServiceStub client;

    @Inject
    public DefaultTitusAgentSecurityGroupService(GrpcConfiguration grpcConfiguration,
                                                 TitusAgentSecurityGroupServiceStub client) {
        this.grpcConfiguration = grpcConfiguration;
        this.client = client;
    }

    @Override
    public ResetSecurityGroupResponse ResetSecurityGroup(ResetSecurityGroupRequest request, CallMetadata callMetadata) {
        return ResetSecurityGroup(request, callMetadata);
    }
}
