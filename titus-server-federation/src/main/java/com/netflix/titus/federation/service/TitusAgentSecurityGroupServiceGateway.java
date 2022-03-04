package com.netflix.titus.federation.service;

import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc.TitusAgentSecurityGroupServiceStub;
import com.netflix.titus.TitusVpcApi;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupService;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupRequest;
import rx.Observable;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

public class TitusAgentSecurityGroupServiceGateway implements TitusAgentSecurityGroupService {
    private final TitusAgentSecurityGroupServiceStub clientToVpcService;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public TitusAgentSecurityGroupServiceGateway(VpcServiceConnector connector,
                                                 GrpcConfiguration grpcConfiguration) {
        this.clientToVpcService = TitusAgentSecurityGroupServiceGrpc.newStub(connector.getChannel());
        this.grpcConfiguration = grpcConfiguration;
    }

    @Override
    public Observable<TitusVpcApi.ResetSecurityGroupResponse> ResetSecurityGroup(
            TitusVpcApi.ResetSecurityGroupRequest request, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<ResetSecurityGroupResponse> streamObserver;
            streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    response -> emitter.onNext(response),
                    emitter::onError,
                    emitter::onCompleted
            );
            wrap(clientToVpcService,
                    callMetadata,
                    grpcConfiguration.getRequestTimeoutMs()).resetSecurityGroup(request, streamObserver);
        }, grpcConfiguration.getRequestTimeoutMs());
    }

    private TitusAgentSecurityGroupServiceStub wrap(TitusAgentSecurityGroupServiceStub client,
                                                    CallMetadata callMetadata, long timeoutMs) {
        return createWrappedStub(client, callMetadata, timeoutMs);
    }
}
