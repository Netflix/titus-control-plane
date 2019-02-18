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

package com.netflix.titus.runtime.connector.common.reactor;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import static com.netflix.titus.runtime.connector.common.reactor.GrpcToReactUtil.toMethodNameFromFullName;

public class ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>> {

    /**
     * For request/response GRPC calls, we set execution deadline at both Reactor and GRPC level. As we prefer the timeout
     * be triggered by GRPC, which may give us potentially more insight, we adjust Reactor timeout value by this factor.
     */
    private static final double RX_CLIENT_TIMEOUT_FACTOR = 1.2;

    /**
     * Event streams have unbounded lifetime, but we want to terminate them periodically to improve request distribution
     * across multiple nodes.
     */
    private static final Duration DEFAULT_STREAMING_TIMEOUT = Duration.ofMinutes(30);

    private final Class<REACT_API> reactApi;
    private final GRPC_STUB grpcStub;
    private final ServiceDescriptor grpcServiceDescriptor;

    private Duration timeout;
    private Duration reactorTimeout;
    private Duration streamingTimeout = DEFAULT_STREAMING_TIMEOUT;
    private CallMetadataResolver callMetadataResolver;

    private ReactorToGrpcClientBuilder(Class<REACT_API> reactApi, GRPC_STUB grpcStub, ServiceDescriptor grpcServiceDescriptor) {
        this.reactApi = reactApi;
        this.grpcStub = grpcStub;
        this.grpcServiceDescriptor = grpcServiceDescriptor;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> withTimeout(Duration timeout) {
        this.timeout = timeout;
        this.reactorTimeout = Duration.ofMillis((long) (timeout.toMillis() * RX_CLIENT_TIMEOUT_FACTOR));
        return this;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> withStreamingTimeout(Duration streamingTimeout) {
        this.streamingTimeout = streamingTimeout;
        return this;
    }

    public ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> withCallMetadataResolver(CallMetadataResolver callMetadataResolver) {
        this.callMetadataResolver = callMetadataResolver;
        return this;
    }

    public static <REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>> ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> newBuilder(
            Class<REACT_API> reactApi,
            GRPC_STUB grpcStub,
            ServiceDescriptor grpcServiceDescriptor) {
        Preconditions.checkArgument(reactApi.isInterface(), "Interface type required");
        return new ReactorToGrpcClientBuilder<>(reactApi, grpcStub, grpcServiceDescriptor);
    }

    public static <REACT_API, GRPC_STUB extends AbstractStub<GRPC_STUB>> ReactorToGrpcClientBuilder<REACT_API, GRPC_STUB> newBuilderWithDefaults(
            Class<REACT_API> reactApi,
            GRPC_STUB grpcStub,
            ServiceDescriptor grpcServiceDescriptor) {
        Preconditions.checkArgument(reactApi.isInterface(), "Interface type required");
        return new ReactorToGrpcClientBuilder<>(reactApi, grpcStub, grpcServiceDescriptor)
                .withTimeout(Duration.ofSeconds(10))
                .withCallMetadataResolver(AnonymousCallMetadataResolver.getInstance());
    }

    public REACT_API build() {
        Preconditions.checkNotNull(timeout, "GRPC request timeout not set");
        Preconditions.checkNotNull(streamingTimeout, "GRPC streaming request timeout not set");
        Preconditions.checkNotNull(callMetadataResolver, "Call metadata resolver not set");

        Map<Method, Function<Object[], Publisher>> methodMap = buildMethodMap();
        return (REACT_API) Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[]{reactApi},
                new ReactorClientInvocationHandler(reactApi, methodMap)
        );
    }

    private Map<Method, Function<Object[], Publisher>> buildMethodMap() {
        Supplier<GRPC_STUB> grpcRequestResponseStubSupplier = buildGrpcRequestResponseStubSupplier();
        Supplier<GRPC_STUB> grpcStreamingStubSupplier = buildGrpcStreamingStubSupplier();

        Map<Method, Function<Object[], Publisher>> mapping = new HashMap<>();
        for (Method method : reactApi.getMethods()) {
            mapping.put(method, createGrpcBridge(method, grpcRequestResponseStubSupplier, grpcStreamingStubSupplier));
        }
        return mapping;
    }

    private Supplier<GRPC_STUB> buildGrpcRequestResponseStubSupplier() {
        Supplier<GRPC_STUB> streamingStubSupplier = buildGrpcStreamingStubSupplier();
        return () -> streamingStubSupplier.get().withDeadline(Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS));
    }

    private Supplier<GRPC_STUB> buildGrpcStreamingStubSupplier() {
        return () ->
                callMetadataResolver.resolve()
                        .map(callMetadata -> V3HeaderInterceptor.attachCallMetadata(grpcStub, callMetadata))
                        .orElse(grpcStub)
                        .withDeadline(Deadline.after(streamingTimeout.toMillis(), TimeUnit.MILLISECONDS));
    }

    private Function<Object[], Publisher> createGrpcBridge(Method reactMethod,
                                                           Supplier<GRPC_STUB> grpcRequestResponseStubSupplier,
                                                           Supplier<GRPC_STUB> grpcStreamingStubSupplier) {
        Method grpcMethod = GrpcToReactUtil.getGrpcMethod(grpcStub, reactMethod);
        if (reactMethod.getReturnType().isAssignableFrom(Mono.class)) {
            return new MonoMethodBridge<>(reactMethod, grpcMethod, grpcRequestResponseStubSupplier, reactorTimeout);
        }
        boolean streamingResponse = grpcServiceDescriptor.getMethods().stream()
                .filter(m -> toMethodNameFromFullName(m.getFullMethodName()).equals(reactMethod.getName()))
                .findFirst()
                .map(m -> m.getType() == MethodDescriptor.MethodType.SERVER_STREAMING)
                .orElse(false);
        return new FluxMethodBridge<>(
                reactMethod,
                grpcMethod,
                streamingResponse ? grpcStreamingStubSupplier : grpcRequestResponseStubSupplier,
                streamingResponse,
                reactorTimeout
        );
    }
}
