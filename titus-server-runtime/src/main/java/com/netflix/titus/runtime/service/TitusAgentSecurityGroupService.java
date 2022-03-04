package com.netflix.titus.runtime.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupRequest;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse;
import rx.Observable;

public interface TitusAgentSecurityGroupService {
    Observable<ResetSecurityGroupResponse> ResetSecurityGroup(ResetSecurityGroupRequest request, CallMetadata callMetadata);
}