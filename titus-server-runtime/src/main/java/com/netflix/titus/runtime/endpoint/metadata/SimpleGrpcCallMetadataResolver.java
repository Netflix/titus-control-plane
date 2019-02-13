/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.runtime.endpoint.metadata;

import java.util.Collections;
import java.util.Optional;
import javax.inject.Singleton;

import com.netflix.titus.common.util.CollectionsExt;
import io.grpc.Context;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;
import static java.util.Arrays.asList;

@Singleton
public class SimpleGrpcCallMetadataResolver implements CallMetadataResolver {

    @Override
    public Optional<CallMetadata> resolve() {
        if (Context.current() == Context.ROOT) {
            // Not in GRPC server call.
            return Optional.empty();
        }

        CallMetadata forwardedCallMetadata = V3HeaderInterceptor.CALL_METADATA_CONTEXT_KEY.get();
        Caller directCaller = resolveDirectCallerInternal();

        // If we have CallMetadata instance, we can safely ignore other headers, except the direct caller.
        if (forwardedCallMetadata != null) {
            return Optional.of(forwardedCallMetadata.toBuilder()
                    .withCallPath(CollectionsExt.copyAndAdd(forwardedCallMetadata.getCallPath(), directCaller.getId()))
                    .withCallers(CollectionsExt.copyAndAdd(forwardedCallMetadata.getCallers(), directCaller))
                    .build());
        }

        // No CellMetadata in header, so we must built it here.
        String callerId = V3HeaderInterceptor.CALLER_ID_CONTEXT_KEY.get();
        String callReason = getOrDefault(V3HeaderInterceptor.CALL_REASON_CONTEXT_KEY.get(), "reason not given");

        CallMetadata.Builder callMetadataBuilder = CallMetadata.newBuilder().withCallReason(callReason);

        if (callerId == null) {
            callMetadataBuilder
                    .withCallerId(directCaller.getId())
                    .withCallPath(Collections.singletonList(directCaller.getId()))
                    .withCallers(Collections.singletonList(directCaller));
        } else {
            Caller originalCaller = Caller.newBuilder()
                    .withId(callerId)
                    .withCallerType(CallerType.parseCallerType(callerId, V3HeaderInterceptor.CALLER_TYPE_CONTEXT_KEY.get()))
                    .build();

            callMetadataBuilder
                    .withCallerId(callerId)
                    .withCallPath(asList(callerId, directCaller.getId()))
                    .withCallers(asList(originalCaller, directCaller));
        }

        return Optional.of(callMetadataBuilder.build());
    }

    protected Optional<Caller> resolveDirectCaller() {
        return Optional.empty();
    }

    private Caller resolveDirectCallerInternal() {
        return resolveDirectCaller().orElseGet(() ->
                {
                    String directCallerId = getOrDefault(V3HeaderInterceptor.DIRECT_CALLER_ID_CONTEXT_KEY.get(), CallMetadataUtils.UNKNOWN_CALLER_ID);
                    CallerType directCallerType = CallerType.parseCallerType(directCallerId, V3HeaderInterceptor.DIRECT_CALLER_TYPE_CONTEXT_KEY.get());
                    return Caller.newBuilder()
                            .withId(directCallerId)
                            .withCallerType(directCallerType)
                            .withContext(V3HeaderInterceptor.CALLER_CONTEXT_CONTEXT_KEY.get())
                            .build();
                }
        );
    }
}
