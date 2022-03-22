package com.netflix.titus.runtime.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupRequest;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse;
import reactor.core.publisher.Mono;

public interface TitusAgentSecurityGroupClient {
    Mono<ResetSecurityGroupResponse> resetSecurityGroup(ResetSecurityGroupRequest request, CallMetadata callMetadata);
}