package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.AbstractStub;

public class DefaultGrpcClientCallAssistantFactory implements GrpcClientCallAssistantFactory {

    private final GrpcCallAssistantConfiguration configuration;
    private final CallMetadataResolver callMetadataResolver;

    public DefaultGrpcClientCallAssistantFactory(GrpcCallAssistantConfiguration configuration,
                                                 CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public <STUB extends AbstractStub<STUB>> GrpcClientCallAssistant<STUB> create(STUB stub) {
        return new DefaultGrpcClientCallAssistant<>(
                configuration,
                stub,
                callMetadataResolver
        );
    }
}
