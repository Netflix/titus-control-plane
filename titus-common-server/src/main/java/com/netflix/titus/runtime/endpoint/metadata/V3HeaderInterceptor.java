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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.endpoint.common.grpc.CommonRuntimeGrpcModelConverters;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

/**
 *
 */
public class V3HeaderInterceptor implements ServerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(V3HeaderInterceptor.class);

    private static final String UNKNOWN_IP = "unknown";

    private static final String X_TITUS_GRPC_CALLER_CONTEXT = "X-Titus-GrpcCallerContext";

    private static final Set<String> ALLOWED_COMPRESSION_TYPES = asSet("gzip");

    public static Metadata.Key<String> DEBUG_KEY = Metadata.Key.of(CallMetadataHeaders.DEBUG_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> COMPRESSION_KEY = Metadata.Key.of(CallMetadataHeaders.COMPRESSION_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> CALLER_ID_KEY = Metadata.Key.of(CallMetadataHeaders.CALLER_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> CALLER_TYPE_KEY = Metadata.Key.of(CallMetadataHeaders.CALLER_TYPE_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> DIRECT_CALLER_ID_KEY = Metadata.Key.of(CallMetadataHeaders.DIRECT_CALLER_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<String> CALL_REASON_KEY = Metadata.Key.of(CallMetadataHeaders.CALL_REASON_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static Metadata.Key<byte[]> CALL_METADATA_KEY = Metadata.Key.of(CallMetadataHeaders.CALL_METADATA_HEADER, Metadata.BINARY_BYTE_MARSHALLER);

    public static Context.Key<String> DEBUG_CONTEXT_KEY = Context.key(CallMetadataHeaders.DEBUG_HEADER);
    public static Context.Key<String> COMPRESSION_CONTEXT_KEY = Context.key(CallMetadataHeaders.COMPRESSION_HEADER);
    public static Context.Key<String> CALLER_ID_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALLER_ID_HEADER);
    public static Context.Key<String> CALLER_TYPE_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALLER_TYPE_HEADER);
    public static Context.Key<String> DIRECT_CALLER_ID_CONTEXT_KEY = Context.key(CallMetadataHeaders.DIRECT_CALLER_ID_HEADER);
    public static Context.Key<String> DIRECT_CALLER_TYPE_CONTEXT_KEY = Context.key(CallMetadataHeaders.DIRECT_CALLER_TYPE_HEADER);
    public static Context.Key<String> CALL_REASON_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALL_REASON_HEADER);
    public static Context.Key<Map<String, String>> CALLER_CONTEXT_CONTEXT_KEY = Context.key(X_TITUS_GRPC_CALLER_CONTEXT);
    public static Context.Key<CallMetadata> CALL_METADATA_CONTEXT_KEY = Context.key(CallMetadataHeaders.CALL_METADATA_HEADER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Context wrappedContext = Context.current();

        Object debugValue = headers.get(DEBUG_KEY);
        if (debugValue != null) {
            boolean debugEnabled = Boolean.parseBoolean(debugValue.toString());
            if (debugEnabled) {
                wrappedContext = wrappedContext.withValue(DEBUG_CONTEXT_KEY, "true");
            }
        }
        Object compressionValue = headers.get(COMPRESSION_KEY);
        if (compressionValue != null) {
            String compressionType = compressionValue.toString();
            if (ALLOWED_COMPRESSION_TYPES.contains(compressionType)) {
                call.setCompression(compressionType);
                wrappedContext = wrappedContext.withValue(COMPRESSION_CONTEXT_KEY, compressionType);
            }
        }

        wrappedContext = copyIntoContext(wrappedContext, headers, CALLER_ID_KEY, CALLER_ID_CONTEXT_KEY);
        wrappedContext = copyIntoContext(wrappedContext, headers, CALLER_TYPE_KEY, CALLER_TYPE_CONTEXT_KEY);
        wrappedContext = copyIntoContext(wrappedContext, headers, DIRECT_CALLER_ID_KEY, DIRECT_CALLER_ID_CONTEXT_KEY);
        wrappedContext = copyIntoContext(wrappedContext, headers, CALL_REASON_KEY, CALL_REASON_CONTEXT_KEY);
        wrappedContext = copyDirectCallerContextIntoContext(wrappedContext, call);

        Object callMetadataValue = headers.get(CALL_METADATA_KEY);
        if (callMetadataValue != null) {
            try {
                com.netflix.titus.grpc.protogen.CallMetadata grpcCallMetadata = com.netflix.titus.grpc.protogen.CallMetadata.parseFrom((byte[]) callMetadataValue);
                wrappedContext = wrappedContext.withValue(CALL_METADATA_CONTEXT_KEY, CommonRuntimeGrpcModelConverters.toCallMetadata(grpcCallMetadata));
            } catch (Exception e) {
                // Ignore bad header value.
                logger.info("Invalid CallMetadata in a request header", e);
            }
        }

        return wrappedContext == Context.current()
                ? next.startCall(call, headers)
                : Contexts.interceptCall(wrappedContext, call, headers, next);
    }

    public static <STUB extends AbstractStub<STUB>> STUB attachCallMetadata(STUB serviceStub, CallMetadata callMetadata) {
        Metadata metadata = new Metadata();
        metadata.put(CALL_METADATA_KEY, CommonRuntimeGrpcModelConverters.toGrpcCallMetadata(callMetadata).toByteArray());
        return serviceStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    private static Context copyIntoContext(Context context, Metadata headers, Metadata.Key<String> headerKey, Context.Key<String> contextKey) {
        Object value = headers.get(headerKey);
        return value == null ? context : context.withValue(contextKey, value.toString());
    }

    private <ReqT, RespT> Context copyDirectCallerContextIntoContext(Context context, ServerCall<ReqT, RespT> call) {
        Map<String, String> callerContext = new HashMap<>();

        String fullName = call.getMethodDescriptor().getFullMethodName();
        int methodBegin = fullName.indexOf('/');
        String serviceName;
        String methodName;
        if (methodBegin <= 0) {
            serviceName = fullName;
            methodName = fullName;
        } else {
            serviceName = fullName.substring(0, methodBegin);
            methodName = Character.toLowerCase(fullName.charAt(methodBegin + 1)) + fullName.substring(methodBegin + 2);
        }

        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_SERVICE_NAME, serviceName);
        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_SERVICE_METHOD, methodName);
        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_TRANSPORT_TYPE, "GRPC");
        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_TRANSPORT_SECURE, "?");

        String callerAddress = processAddress(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)).getLeft();
        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_CALLER_ADDRESS, callerAddress);

        Pair<String, Integer> localIpAndPort = processAddress(call.getAttributes().get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR));
        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_LOCAL_ADDRESS, localIpAndPort.getLeft());
        callerContext.put(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_LOCAL_PORT, "" + localIpAndPort.getRight());

        return context.withValue(CALLER_CONTEXT_CONTEXT_KEY, callerContext);
    }

    private Pair<String, Integer> processAddress(@Nullable final SocketAddress socketAddress) {
        if (socketAddress == null) {
            return Pair.of(UNKNOWN_IP, 0);
        }

        if (!(socketAddress instanceof InetSocketAddress)) {
            return Pair.of(socketAddress.toString(), 0);
        }

        final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        final String hostString = inetSocketAddress.getHostString();
        return Pair.of(hostString == null ? UNKNOWN_IP : hostString, inetSocketAddress.getPort());
    }
}
