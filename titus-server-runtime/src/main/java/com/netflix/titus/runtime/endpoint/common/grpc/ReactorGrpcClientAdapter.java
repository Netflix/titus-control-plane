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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class provides utilities for bridging GRPC client calls with Reactor Rx streams.
 */
public class ReactorGrpcClientAdapter<CLIENT extends AbstractStub<CLIENT>> {

    private static final String CANCELLING_MESSAGE = "Cancelling the call";

    /**
     * For request/response GRPC calls, we set execution deadline at both RxJava and GRPC level. As we prefer the timeout
     * be triggered by GRPC, which may give us potentially more insight, we adjust RxJava timeout value by this factor.
     */
    private static final double RX_CLIENT_TIMEOUT_FACTOR = 1.2;

    private final CLIENT baseClient;
    private final CallMetadataResolver callMetadataResolver;
    private final GrpcClientConfiguration configuration;

    public ReactorGrpcClientAdapter(CLIENT client,
                                    CallMetadataResolver callMetadataResolver,
                                    GrpcClientConfiguration configuration) {
        this.callMetadataResolver = callMetadataResolver;
        this.configuration = configuration;
        this.baseClient = client;
    }

    public Mono<Void> asVoidMono(BiConsumer<CLIENT, StreamObserver<Void>> invocationHandler) {
        return asMono(invocationHandler);
    }

    public <RESP> Mono<RESP> asMono(BiConsumer<CLIENT, StreamObserver<RESP>> invocationHandler) {
        // Fetch scope eagerly, as it is part of the thread context.
        CLIENT client = newRequestScopeClient(newClientWithTimeout());

        Mono<RESP> mono = Mono.create(sink -> {

            // Mono combines onNext with onCompleted so we need to store the value until GRPC onCompleted arrives
            AtomicReference<RESP> responseRef = new AtomicReference<>();

            ClientResponseObserver<Object, RESP> responseStream = new ClientResponseObserver<Object, RESP>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<Object> requestStream) {
                    sink.onCancel(() -> requestStream.cancel(CANCELLING_MESSAGE, null));
                }

                @Override
                public void onNext(RESP value) {
                    responseRef.set(value);
                }

                @Override
                public void onError(Throwable error) {
                    try {
                        sink.error(error);
                    } catch (Exception ignore) {
                    }
                }

                @Override
                public void onCompleted() {
                    try {
                        if (responseRef.get() == null) {
                            sink.success();
                        } else {
                            sink.success(responseRef.get());
                        }
                    } catch (Exception ignore) {
                    }
                }
            };

            invocationHandler.accept(client, responseStream);
        });

        return applyRxTimeout(mono);
    }

    public <RESP> Flux<RESP> asFlux(BiConsumer<CLIENT, StreamObserver<RESP>> invocationHandler) {
        // Fetch scope eagerly, as it is part of the thread context.
        CLIENT client = newRequestScopeClient(baseClient);

        Flux<RESP> flux = Flux.create(sink -> {
            ClientResponseObserver<Object, RESP> responseStream = new ClientResponseObserver<Object, RESP>() {

                private ClientCallStreamObserver<Object> requestStream;

                @Override
                public void beforeStart(ClientCallStreamObserver<Object> requestStream) {
                    this.requestStream = requestStream;
                    sink.onCancel(() -> requestStream.cancel(CANCELLING_MESSAGE, null));
                }

                @Override
                public void onNext(RESP value) {
                    try {
                        sink.next(value);
                    } catch (Exception e) {
                        requestStream.cancel("Client onNext error", e);
                    }
                }

                @Override
                public void onError(Throwable error) {
                    try {
                        sink.error(error);
                    } catch (Exception ignore) {
                    }
                }

                @Override
                public void onCompleted() {
                    try {
                        sink.complete();
                    } catch (Exception ignore) {
                    }
                }
            };

            invocationHandler.accept(client, responseStream);
        });

        return flux;
    }

    private CLIENT newClientWithTimeout() {
        return baseClient.withDeadlineAfter(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    private CLIENT newRequestScopeClient(CLIENT client) {
        return callMetadataResolver.resolve()
                .map(caller -> V3HeaderInterceptor.attachCallMetadata(client, caller))
                .orElse(baseClient);
    }

    private <RESP> Mono<RESP> applyRxTimeout(Mono<RESP> mono) {
        return mono.timeout(Duration.ofMillis(getRxJavaAdjustedTimeout()));
    }

    private long getRxJavaAdjustedTimeout() {
        return (long) (configuration.getRequestTimeout() * RX_CLIENT_TIMEOUT_FACTOR);
    }
}
