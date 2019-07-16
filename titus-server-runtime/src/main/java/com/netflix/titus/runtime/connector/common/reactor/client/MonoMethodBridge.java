/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.common.reactor.client;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.Deadline;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

class MonoMethodBridge<GRPC_STUB extends AbstractStub<GRPC_STUB>> implements Function<Object[], Publisher> {

    private final Method grpcMethod;
    private final int grpcArgPos;
    private final int callMetadataPos;
    private final CallMetadataResolver callMetadataResolver;
    private final GRPC_STUB grpcStub;
    private final boolean emptyToVoidReply;
    private final Duration timeout;
    private final Duration reactorTimeout;

    /**
     * If grpcArgPos is less then zero, it means no GRPC argument is provided, and instead {@link Empty} value should be used.
     * If callMetadataPos is less then zero, it means the {@link CallMetadata} value should be resolved from the local context.
     */
    MonoMethodBridge(Method reactMethod,
                     Method grpcMethod,
                     int grpcArgPos,
                     int callMetadataPos,
                     CallMetadataResolver callMetadataResolver,
                     GRPC_STUB grpcStub,
                     Duration timeout) {
        this.grpcMethod = grpcMethod;
        this.grpcArgPos = grpcArgPos;
        this.callMetadataPos = callMetadataPos;
        this.callMetadataResolver = callMetadataResolver;
        this.grpcStub = grpcStub;
        this.emptyToVoidReply = GrpcToReactUtil.isEmptyToVoidResult(reactMethod, grpcMethod);
        this.timeout = timeout;
        this.reactorTimeout = Duration.ofMillis((long) (timeout.toMillis() * GrpcToReactUtil.RX_CLIENT_TIMEOUT_FACTOR));
    }

    @Override
    public Publisher apply(Object[] args) {
        return Mono.create(sink -> new MonoInvocation(sink, args)).timeout(reactorTimeout);
    }

    private class MonoInvocation {

        private MonoInvocation(MonoSink<Object> sink, Object[] args) {
            StreamObserver<Object> grpcStreamObserver = new ClientResponseObserver<Object, Object>() {
                @Override
                public void beforeStart(ClientCallStreamObserver requestStream) {
                    sink.onCancel(() -> requestStream.cancel("React subscription cancelled", null));
                }

                @Override
                public void onNext(Object value) {
                    if (emptyToVoidReply) {
                        sink.success();
                    } else {
                        sink.success(value);
                    }
                }

                @Override
                public void onError(Throwable error) {
                    sink.error(error);
                }

                @Override
                public void onCompleted() {
                    sink.success();
                }
            };

            Object[] grpcArgs = new Object[]{
                    grpcArgPos < 0 ? Empty.getDefaultInstance() : args[grpcArgPos],
                    grpcStreamObserver
            };

            GRPC_STUB invocationStub = handleCallMetadata(args)
                    .withDeadline(Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS));

            try {
                grpcMethod.invoke(invocationStub, grpcArgs);
            } catch (Exception e) {
                sink.error(e);
            }
        }

        private GRPC_STUB handleCallMetadata(Object[] args) {
            if (callMetadataPos >= 0) {
                return V3HeaderInterceptor.attachCallMetadata(grpcStub, (CallMetadata) args[callMetadataPos]);
            }
            return callMetadataResolver.resolve()
                    .map(callMetadata -> V3HeaderInterceptor.attachCallMetadata(grpcStub, callMetadata))
                    .orElse(grpcStub);
        }
    }
}
