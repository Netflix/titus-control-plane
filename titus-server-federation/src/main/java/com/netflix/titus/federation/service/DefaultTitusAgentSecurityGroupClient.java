package com.netflix.titus.federation.service;

import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc;
import com.netflix.titus.TitusAgentSecurityGroupServiceGrpc.TitusAgentSecurityGroupServiceStub;
import com.netflix.titus.TitusVpcApi;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.runtime.endpoint.v3.grpc.DefaultJobActivityHistoryServiceGrpc;
import com.netflix.titus.runtime.service.TitusAgentSecurityGroupClient;
import com.netflix.titus.TitusVpcApi.ResetSecurityGroupResponse;
import org.bouncycastle.asn1.its.IValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

public class DefaultTitusAgentSecurityGroupClient implements TitusAgentSecurityGroupClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultTitusAgentSecurityGroupClient.class);
    private final TitusAgentSecurityGroupServiceStub clientToVpcService;
    private final GrpcConfiguration grpcConfiguration;

    @Inject
    public DefaultTitusAgentSecurityGroupClient(VpcServiceConnector connector,
                                                GrpcConfiguration grpcConfiguration) {
        this.clientToVpcService = TitusAgentSecurityGroupServiceGrpc.newStub(connector.getChannel());
        this.grpcConfiguration = grpcConfiguration;
    }

    @Override
    public Mono<ResetSecurityGroupResponse> resetSecurityGroup(
            TitusVpcApi.ResetSecurityGroupRequest request, CallMetadata callMetadata) {

        return Mono.create(sink -> {
            TitusAgentSecurityGroupServiceStub clientStub = wrap(clientToVpcService,
                    callMetadata,
                    grpcConfiguration.getRequestTimeoutMs());
            clientStub.resetSecurityGroup(request, new StreamObserver<ResetSecurityGroupResponse>() {
                private ResetSecurityGroupResponse val;
                @Override
                public void onNext(ResetSecurityGroupResponse value) {
                    val = value;
                }

                @Override
                public void onError(Throwable t) {
                    sink.error(t);
                }

                @Override
                public void onCompleted() {
                    sink.success(val);
                }
            });
        });
    }

    private TitusAgentSecurityGroupServiceStub wrap(TitusAgentSecurityGroupServiceStub client,
                                                    CallMetadata callMetadata, long timeoutMs) {
        return createWrappedStub(client, callMetadata, timeoutMs);
    }
}
