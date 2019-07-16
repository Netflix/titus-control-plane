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

package com.netflix.titus.runtime.connector.common.reactor.server;

import com.google.protobuf.Empty;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

class UnaryMethodHandler<REQ, RESP> extends AbstractMethodHandler<REQ, RESP> implements io.grpc.stub.ServerCalls.UnaryMethod<REQ, RESP> {

    private static final Logger logger = LoggerFactory.getLogger(UnaryMethodHandler.class);

    UnaryMethodHandler(GrpcToReactorMethodBinding<REQ, RESP> binding,
                       CallMetadataResolver callMetadataResolver,
                       Object reactorService) {
        super(binding, callMetadataResolver, reactorService);
    }

    @Override
    public void invoke(REQ request, StreamObserver<RESP> responseObserver) {
        super.invoke(request, responseObserver);
    }

    @Override
    Disposable handleResult(Publisher<RESP> result, StreamObserver<RESP> responseObserver) {
        Mono<RESP> monoResult = (Mono<RESP>) result;
        Disposable disposable;
        if (binding.getReturnTypeParameter() == Void.class) {
            disposable = monoResult.subscribe(
                    next -> {
                    },
                    e -> {
                        try {
                            responseObserver.onError(e);
                        } catch (Exception ex) {
                            logger.warn("Subscriber threw error in onError handler", ex);
                        }
                    },
                    () -> {
                        try {
                            // Void must be mapped to GRPC/Empty value.
                            responseObserver.onNext((RESP) Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        } catch (Exception ex) {
                            logger.warn("Subscriber threw error in onCompleted handler. Retrying with onError", ex);
                            try {
                                responseObserver.onError(ex);
                            } catch (Exception ignore) {
                            }
                        }
                    }
            );
        } else {
            disposable = monoResult.subscribe(
                    responseObserver::onNext,
                    e -> {
                        try {
                            responseObserver.onError(e);
                        } catch (Exception ex) {
                            logger.warn("Subscriber threw error in onError handler", ex);
                        }
                    },
                    () -> {
                        try {
                            responseObserver.onCompleted();
                        } catch (Exception ex) {
                            logger.warn("Subscriber threw error in onCompleted handler. Retrying with onError", ex);
                            try {
                                responseObserver.onError(ex);
                            } catch (Exception ignore) {
                            }
                        }
                    }
            );
        }
        return disposable;
    }
}
