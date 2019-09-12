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

package com.netflix.titus.common.util.grpc.reactor.server;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.util.rx.ReactorExt;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

class ServerStreamingMethodHandler<REQ, RESP, CONTEXT> extends AbstractMethodHandler<REQ, RESP, CONTEXT> implements ServerCalls.ServerStreamingMethod<REQ, RESP> {

    private static final Logger logger = LoggerFactory.getLogger(ServerStreamingMethodHandler.class);

    ServerStreamingMethodHandler(GrpcToReactorMethodBinding<REQ, RESP> binding,
                                 Supplier<CONTEXT> contextResolver,
                                 Object reactorService) {
        super(binding, contextResolver, reactorService);
    }

    @Override
    public void invoke(REQ request, StreamObserver<RESP> responseObserver) {
        super.invoke(request, responseObserver);
    }

    @Override
    Disposable handleResult(Publisher<RESP> result, StreamObserver<RESP> responseObserver) {
        return internalHandleResult(result, responseObserver);
    }

    @VisibleForTesting
    static <RESP> Disposable internalHandleResult(Publisher<RESP> result, StreamObserver<RESP> responseObserver) {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Disposable disposable = Flux.from(result).subscribe(
                value -> {
                    if (cancelled.get()) {
                        ReactorExt.safeDispose(disposableRef.get());
                        return;
                    }
                    try {
                        responseObserver.onNext(value);
                    } catch (Exception e) {
                        cancelled.set(true);

                        logger.warn("Subscriber threw error in onNext handler. Retrying with onError", e);
                        try {
                            responseObserver.onError(e);
                        } catch (Exception e2) {
                            logger.warn("Subscriber threw error in onError handler", e2);
                        }

                        ReactorExt.safeDispose(disposableRef.get());
                    }
                },
                e -> {
                    if (cancelled.get()) {
                        return;
                    }
                    try {
                        responseObserver.onError(e);
                    } catch (Exception e2) {
                        logger.warn("Subscriber threw error in onError handler", e2);
                    }
                },
                () -> {
                    if (cancelled.get()) {
                        return;
                    }
                    try {
                        responseObserver.onCompleted();
                    } catch (Exception e) {
                        logger.warn("Subscriber threw error in onCompleted handler", e);
                    }
                }
        );
        disposableRef.set(disposable);

        if (cancelled.get()) {
            ReactorExt.safeDispose(disposable);
        }

        return disposable;
    }
}
