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

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.protobuf.Empty;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import rx.Emitter;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;

public class GrpcUtil {

    private static final String CANCELLING_MESSAGE = "Cancelling the call";

    public static <T> void safeOnError(Logger logger, Throwable error, StreamObserver<T> responseObserver) {
        try {
            responseObserver.onError(error);
        } catch (Exception e) {
            if (e instanceof IllegalStateException) { // Stream most likely closed, which is ok
                logger.info(e.getMessage());
            } else {
                logger.error("Error during writing error to StreamObserver", e);
            }
        }
    }

    public static <T> StreamObserver<T> createSimpleStreamObserver(Emitter<T> emitter) {
        return createStreamObserver(
                emitter::onNext,
                emitter::onError,
                emitter::onCompleted
        );
    }

    public static StreamObserver<Empty> createEmptyStreamObserver(Emitter emitter) {
        return createStreamObserver(
                ignored -> {
                },
                emitter::onError,
                emitter::onCompleted
        );
    }

    public static <T> StreamObserver<T> createStreamObserver(final Action1<? super T> onNext, final Action1<Throwable> onError, final Action0 onCompleted) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(T value) {
                onNext.call(value);
            }

            @Override
            public void onError(Throwable t) {
                onError.call(t);
            }

            @Override
            public void onCompleted() {
                onCompleted.call();
            }
        };
    }

    public static <STUB extends AbstractStub<STUB>> STUB createWrappedStub(SessionContext sessionContext, STUB client) {
        return sessionContext.getCallerId()
                .map(callerId -> V3HeaderInterceptor.attachCallerId(client, callerId + ",TitusGateway"))
                .orElse(client);
    }

    public static <O> Observable<O> toObservable(BiConsumer<Empty, StreamObserver<O>> grpcServiceMethod) {
        return toObservable(Empty.getDefaultInstance(), grpcServiceMethod);
    }

    public static <I, O> Observable<O> toObservable(I input, BiConsumer<I, StreamObserver<O>> grpcServiceMethod) {
        return Observable.create(emitter -> {
            StreamObserver<O> streamObserver = new StreamObserver<O>() {
                @Override
                public void onNext(O value) {
                    emitter.onNext(value);
                }

                @Override
                public void onError(Throwable t) {
                    emitter.onError(t);
                }

                @Override
                public void onCompleted() {
                    emitter.onCompleted();
                }
            };
            grpcServiceMethod.accept(input, streamObserver);
        }, Emitter.BackpressureMode.NONE);
    }

    public static <STUB extends AbstractStub<STUB>, ReqT, RespT> ClientCall call(SessionContext sessionContext,
                                                                                 STUB client,
                                                                                 MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                                                 ReqT request,
                                                                                 long deadlineMs,
                                                                                 StreamObserver<RespT> responseObserver) {
        STUB wrappedStub = GrpcUtil.createWrappedStub(sessionContext, client);
        CallOptions callOptions = wrappedStub.getCallOptions().withDeadline(Deadline.after(deadlineMs, TimeUnit.MILLISECONDS));
        ClientCall<ReqT, RespT> clientCall = wrappedStub.getChannel().newCall(methodDescriptor, callOptions);
        asyncUnaryCall(clientCall, request, responseObserver);
        return clientCall;
    }

    public static <STUB extends AbstractStub<STUB>, ReqT, RespT> ClientCall callStreaming(SessionContext sessionContext,
                                                                                          STUB client,
                                                                                          MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                                                          ReqT request,
                                                                                          StreamObserver<RespT> responseObserver) {
        STUB wrappedStub = GrpcUtil.createWrappedStub(sessionContext, client);
        ClientCall<ReqT, RespT> clientCall = wrappedStub.getChannel().newCall(methodDescriptor, wrappedStub.getCallOptions());
        asyncServerStreamingCall(clientCall, request, responseObserver);
        return clientCall;
    }

    public static void attachCancellingCallback(Emitter emitter, ClientCall clientCall) {
        emitter.setCancellation(() -> clientCall.cancel(CANCELLING_MESSAGE, null));
    }

    public static void attachCancellingCallback(StreamObserver responseObserver, Subscription subscription) {
        ServerCallStreamObserver serverObserver = (ServerCallStreamObserver) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }
}
