/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

/**
 *
 */
public class V3HeaderInterceptor implements ServerInterceptor {

    public static String DEBUG_HEADER = "X-Titus-Debug";
    public static String CALLER_ID_HEADER = "X-Titus-CallerId";

    public static Metadata.Key<String> DEBUG_KEY = Metadata.Key.of(DEBUG_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> CALLER_ID_KEY = Metadata.Key.of(CALLER_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Context.Key<String> CALLER_ID_CONTEXT_KEY = Context.key(CALLER_ID_HEADER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Object value = headers.get(CALLER_ID_KEY);
        if (value != null) {
            Context wrappedContext = Context.current().withValue(CALLER_ID_CONTEXT_KEY, value.toString());
            return Contexts.interceptCall(wrappedContext, call, headers, next);
        }

        return next.startCall(call, headers);
    }

    public static <STUB extends AbstractStub<STUB>> STUB attachCallerId(STUB serviceStub, String callerId) {
        Metadata metadata = new Metadata();
        metadata.put(CALLER_ID_KEY, callerId);
        metadata.put(DEBUG_KEY, "true");
        return serviceStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }
}
