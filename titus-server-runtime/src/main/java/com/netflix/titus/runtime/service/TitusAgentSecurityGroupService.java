package com.netflix.titus.runtime.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.grpc.protogen.ResetSecurityGroupResponse;
import com.netflix.titus.grpc.protogen.ResetSecurityGroupRequest;

public interface TitusAgentSecurityGroupService {
    ResetSecurityGroupResponse ResetSecurityGroup(ResetSecurityGroupRequest request, CallMetadata callMetadata);
}
