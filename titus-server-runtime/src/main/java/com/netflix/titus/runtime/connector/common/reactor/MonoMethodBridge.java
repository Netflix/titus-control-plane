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
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.protobuf.Empty;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

class MonoMethodBridge<GRPC_STUB extends AbstractStub<GRPC_STUB>> implements Function<Object[], Publisher> {

    private final Method grpcMethod;
    private final Supplier<GRPC_STUB> grpcStubFactory;
    private final Duration reactorTimeout;

    MonoMethodBridge(Method grpcMethod, Supplier<GRPC_STUB> grpcStubFactory, Duration reactorTimeout) {
        this.grpcMethod = grpcMethod;
        this.grpcStubFactory = grpcStubFactory;
        this.reactorTimeout = reactorTimeout;
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
                    sink.success(value);
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
                    (args == null || args.length == 0) ? Empty.getDefaultInstance() : args[0],
                    grpcStreamObserver
            };
            try {
                grpcMethod.invoke(grpcStubFactory.get(), grpcArgs);
            } catch (Exception e) {
                sink.error(e);
            }
        }
    }
}
