package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import io.grpc.stub.AbstractStub;

public interface GrpcClientCallAssistantFactory {

    <STUB extends AbstractStub<STUB>> GrpcClientCallAssistant<STUB> create(STUB stub);
}
