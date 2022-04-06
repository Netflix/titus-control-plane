package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import com.netflix.titus.api.model.callmetadata.CallMetadata;

public class GrpcServerCallContext {

    private final CallMetadata callMetadata;
    private final String callerAddress;
    private final String callerApplicationName;

    GrpcServerCallContext(CallMetadata callMetadata, String callerAddress, String callerApplicationName) {
        this.callMetadata = callMetadata;
        this.callerAddress = callerAddress;
        this.callerApplicationName = callerApplicationName;
    }

    public CallMetadata getCallMetadata() {
        return callMetadata;
    }

    public String getCallerAddress() {
        return callerAddress;
    }

    public String getCallerApplicationName() {
        return callerApplicationName;
    }
}
