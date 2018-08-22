package com.netflix.titus.runtime.endpoint.metadata;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Singleton;

import com.netflix.titus.common.util.CollectionsExt;
import io.grpc.Context;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;

@Singleton
public class SimpleGrpcCallMetadataResolver implements CallMetadataResolver {

    @Override
    public Optional<CallMetadata> resolve() {
        if (Context.current() == Context.ROOT) {
            // Not in GRPC server call.
            return Optional.empty();
        }

        CallMetadata callMetadata = V3HeaderInterceptor.CALL_METADATA_CONTEXT_KEY.get();
        String directCallerId = resolveDirectCallerId().orElseGet(() ->
                getOrDefault(V3HeaderInterceptor.DIRECT_CALLER_ID_CONTEXT_KEY.get(), CallMetadataUtils.UNKNOWN_CALLER_ID)
        );

        // If we have CallMetadata instance, we can safely ignore other headers, except the direct caller.
        if (callMetadata != null) {
            List<String> callPath = CollectionsExt.copyAndAdd(callMetadata.getCallPath(), directCallerId);
            return Optional.of(callMetadata.toBuilder().withCallPath(callPath).build());
        }

        // No CellMetadata in header, so we must built it here.
        String callerId = getOrDefault(V3HeaderInterceptor.CALLER_ID_CONTEXT_KEY.get(), "unknownCallerId");
        String callReason = getOrDefault(V3HeaderInterceptor.CALL_REASON_CONTEXT_KEY.get(), "reason not given");

        return Optional.of(CallMetadata.newBuilder()
                .withCallerId(callerId)
                .withCallPath(Collections.singletonList(directCallerId))
                .withCallReason(callReason)
                .build()
        );
    }

    protected Optional<String> resolveDirectCallerId() {
        return Optional.empty();
    }
}
