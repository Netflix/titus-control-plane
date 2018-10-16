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
