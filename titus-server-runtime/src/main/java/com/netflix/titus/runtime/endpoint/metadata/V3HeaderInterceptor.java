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

import com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class V3HeaderInterceptor implements ServerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(V3HeaderInterceptor.class);

    public static Metadata.Key<String> DEBUG_KEY = Metadata.Key.of(CallMetadataHeaders.DEBUG_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> CALLER_ID_KEY = Metadata.Key.of(CallMetadataHeaders.CALLER_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> DIRECT_CALLER_ID_KEY = Metadata.Key.of(CallMetadataHeaders.DIRECT_CALLER_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> CALL_REASON_KEY = Metadata.Key.of(CallMetadataHeaders.CALL_REASON_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<byte[]> CALL_METADATA_KEY = Metadata.Key.of(CallMetadataHeaders.CALL_METADATA_HEADER, Metadata.BINARY_BYTE_MARSHALLER);

    public static Context.Key<String> DEBUG_CONTEXT_KEY = Context.key(CallMetadataHeaders.DEBUG_HEADER);
    public static Context.Key<String> CALLER_ID_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALLER_ID_HEADER);
    public static Context.Key<String> DIRECT_CALLER_ID_CONTEXT_KEY = Context.key(CallMetadataHeaders.DIRECT_CALLER_ID_HEADER);
    public static Context.Key<String> CALL_REASON_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALL_REASON_HEADER);
    public static Context.Key<CallMetadata> CALL_METADATA_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALL_METADATA_HEADER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Context wrappedContext = Context.current();

        Object debugValue = headers.get(DEBUG_KEY);
        if (debugValue != null) {
            boolean debugEnabled = "true".equalsIgnoreCase(debugValue.toString());
            if (debugEnabled) {
                wrappedContext = wrappedContext.withValue(DEBUG_CONTEXT_KEY, "true");
            }
        }
        Object callerIdValue = headers.get(CALLER_ID_KEY);
        if (callerIdValue != null) {
            wrappedContext = wrappedContext.withValue(CALLER_ID_CONTEXT_KEY, callerIdValue.toString());
        }
        Object directCallerIdValue = headers.get(DIRECT_CALLER_ID_KEY);
        if (directCallerIdValue != null) {
            wrappedContext = wrappedContext.withValue(DIRECT_CALLER_ID_CONTEXT_KEY, directCallerIdValue.toString());
        }
        Object callReasonValue = headers.get(CALL_REASON_KEY);
        if (callReasonValue != null) {
            wrappedContext = wrappedContext.withValue(CALL_REASON_CONTEXT_KEY, callReasonValue.toString());
        }
        Object callMetadataValue = headers.get(CALL_METADATA_KEY);
        if (callMetadataValue != null) {
            try {
                com.netflix.titus.grpc.protogen.CallMetadata grpcCallMetadata = com.netflix.titus.grpc.protogen.CallMetadata.parseFrom((byte[]) callMetadataValue);
                wrappedContext = wrappedContext.withValue(CALL_METADATA_CONTEXT_KEY, CommonGrpcModelConverters.toCallMetadata(grpcCallMetadata));
            } catch (Exception e) {
                // Ignore bad header value.
                logger.info("Invalid CallMetadata in a request header", e);
            }
        }

        return wrappedContext == Context.current()
                ? next.startCall(call, headers)
                : Contexts.interceptCall(wrappedContext, call, headers, next);
    }

    public static <STUB extends AbstractStub<STUB>> STUB attachCallerId(STUB serviceStub, CallMetadata callMetadata) {
        Metadata metadata = new Metadata();
        metadata.put(CALL_METADATA_KEY, CommonGrpcModelConverters.toGrpcCallMetadata(callMetadata).toByteArray());
        return serviceStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }
}
