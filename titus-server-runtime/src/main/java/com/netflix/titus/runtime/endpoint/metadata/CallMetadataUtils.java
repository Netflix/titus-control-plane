package com.netflix.titus.runtime.endpoint.metadata;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.service.TitusServiceException;
import io.grpc.stub.StreamObserver;

public class CallMetadataUtils {

    public static final String UNKNOWN_CALLER_ID = "unknownDirectCaller";

    public static boolean isUnknown(CallMetadata callMetadata) {
        return UNKNOWN_CALLER_ID.equals(callMetadata.getCallerId());
    }

    /**
     * Execute an action with the resolved call metadata context.
     */
    public static void execute(CallMetadataResolver callMetadataResolver,
                               StreamObserver<?> responseObserver,
                               Consumer<CallMetadata> action) {
        Optional<CallMetadata> callMetadata = callMetadataResolver.resolve();
        if (!callMetadata.isPresent()) {
            responseObserver.onError(TitusServiceException.noCallerId());
            return;
        }
        try {
            action.accept(callMetadata.get());
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    public static Map<String, String> asMap(CallMetadata callMetadata) {
        return ImmutableMap.of(
                "callerId", callMetadata.getCallerId(),
                "callPath", String.join("/", callMetadata.getCallPath()),
                "callReason", callMetadata.getCallReason()
        );
    }
}
