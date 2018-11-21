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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.protobuf.Empty;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;

public class GrpcUtil {

    private static final String CANCELLING_MESSAGE = "Cancelling the call";

    /**
     * For request/response GRPC calls, we set execution deadline at both RxJava and GRPC level. As we prefer the timeout
     * be triggered by GRPC, which may give us potentially more insight, we adjust RxJava timeout value by this factor.
     */
    private static final double RX_CLIENT_TIMEOUT_FACTOR = 1.2;

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

    public static <REQ, RESP> ClientResponseObserver<REQ, RESP> createSimpleClientResponseObserver(Emitter<RESP> emitter) {
        return createClientResponseObserver(
                emitter,
                emitter::onNext,
                emitter::onError,
                emitter::onCompleted
        );
    }

    public static <REQ> ClientResponseObserver<REQ, Empty> createEmptyClientResponseObserver(Emitter<Empty> emitter) {
        return createClientResponseObserver(
                emitter,
                ignored -> {
                },
                emitter::onError,
                emitter::onCompleted
        );
    }

    public static <REQ> ClientResponseObserver<REQ, Empty> createEmptyClientMonoResponse(MonoSink<Empty> monoSink) {
        return createClientResponseObserver(
                requestStream -> monoSink.onCancel(() -> requestStream.cancel(CANCELLING_MESSAGE, null)),
                monoSink::success,
                monoSink::error,
                monoSink::success
        );
    }

    public static <REQ, RESP> ClientResponseObserver<REQ, RESP> createClientResponseObserver(Emitter<?> emitter,
                                                                                             final Action1<? super RESP> onNext,
                                                                                             final Action1<Throwable> onError,
                                                                                             final Action0 onCompleted) {
        return createClientResponseObserver(
                requestStream -> emitter.setCancellation(() -> requestStream.cancel(CANCELLING_MESSAGE, null)),
                onNext,
                onError,
                onCompleted
        );
    }

    public static <REQ, RESP> ClientResponseObserver<REQ, RESP> createClientResponseObserver(final Action1<ClientCallStreamObserver<REQ>> beforeStart,
                                                                                             final Action1<? super RESP> onNext,
                                                                                             final Action1<Throwable> onError,
                                                                                             final Action0 onCompleted) {
        return new ClientResponseObserver<REQ, RESP>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<REQ> requestStream) {
                beforeStart.call(requestStream);
            }

            @Override
            public void onNext(RESP value) {
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

    public static <STUB extends AbstractStub<STUB>> STUB createWrappedStub(STUB client,
                                                                           CallMetadataResolver callMetadataResolver,
                                                                           long deadlineMs) {
        return createWrappedStub(client, callMetadataResolver).withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
    }

    public static <STUB extends AbstractStub<STUB>> STUB createWrappedStub(STUB client, CallMetadataResolver callMetadataResolver) {
        return callMetadataResolver.resolve()
                .map(caller -> V3HeaderInterceptor.attachCallMetadata(client, caller))
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

    public static <STUB extends AbstractStub<STUB>, ReqT, RespT> ClientCall call(CallMetadataResolver callMetadataResolver,
                                                                                 STUB client,
                                                                                 MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                                                 ReqT request,
                                                                                 long deadlineMs,
                                                                                 StreamObserver<RespT> responseObserver) {
        STUB wrappedStub = createWrappedStub(client, callMetadataResolver);
        CallOptions callOptions = wrappedStub.getCallOptions().withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
        ClientCall<ReqT, RespT> clientCall = wrappedStub.getChannel().newCall(methodDescriptor, callOptions);
        asyncUnaryCall(clientCall, request, responseObserver);
        return clientCall;
    }

    public static void attachCancellingCallback(Emitter emitter, ClientCall... clientCalls) {
        emitter.setCancellation(() -> {
            for (ClientCall call : clientCalls) {
                call.cancel(CANCELLING_MESSAGE, null);
            }
        });
    }

    public static void attachCancellingCallback(Emitter emitter, ClientCallStreamObserver... clientCalls) {
        emitter.setCancellation(() -> {
            for (ClientCallStreamObserver call : clientCalls) {
                call.cancel(CANCELLING_MESSAGE, null);
            }
        });
    }

    public static void attachCancellingCallback(StreamObserver responseObserver, Subscription subscription) {
        ServerCallStreamObserver serverObserver = (ServerCallStreamObserver) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    public static void attachCancellingCallback(StreamObserver responseObserver, Disposable disposable) {
        ServerCallStreamObserver serverObserver = (ServerCallStreamObserver) responseObserver;
        serverObserver.setOnCancelHandler(disposable::dispose);
    }

    public static <T> Observable<T> createRequestObservable(Action1<Emitter<T>> emitter) {
        return Observable.create(
                emitter,
                Emitter.BackpressureMode.NONE
        );
    }

    public static <T> Observable<T> createRequestObservable(Action1<Emitter<T>> emitter, long timeout) {
        return createRequestObservable(emitter).timeout(getRxJavaAdjustedTimeout(timeout), TimeUnit.MILLISECONDS);
    }

    public static Completable createRequestCompletable(Action1<Emitter<Empty>> emitter, long timeout) {
        return createRequestObservable(emitter, timeout).toCompletable();
    }

    public static Mono<Empty> createMonoVoidRequest(Consumer<MonoSink<Empty>> sinkConsumer, long timeout) {
        return Mono.create(sinkConsumer).timeout(getRxJavaAdjustedDuration(timeout));
    }

    public static long getRxJavaAdjustedTimeout(long initialTimeoutMs) {
        return (long) (initialTimeoutMs * RX_CLIENT_TIMEOUT_FACTOR);
    }

    public static Duration getRxJavaAdjustedDuration(long initialTimeoutMs) {
        return Duration.ofMillis((long) (initialTimeoutMs * RX_CLIENT_TIMEOUT_FACTOR));
    }

    public static boolean isNotOK(Status oneStatus) {
        return !Status.Code.OK.equals(oneStatus.getCode());
    }
}
