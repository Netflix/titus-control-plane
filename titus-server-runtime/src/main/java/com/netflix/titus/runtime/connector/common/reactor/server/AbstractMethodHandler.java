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

import java.lang.reflect.InvocationTargetException;

import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;

abstract class AbstractMethodHandler<REQ, RESP> {

    private static final Object[] EMPTY_ARG_ARRAY = new Object[0];

    final GrpcToReactorMethodBinding<REQ, RESP> binding;
    private final CallMetadataResolver callMetadataResolver;
    private final Object reactorService;

    AbstractMethodHandler(GrpcToReactorMethodBinding<REQ, RESP> binding,
                          CallMetadataResolver callMetadataResolver,
                          Object reactorService) {
        this.binding = binding;
        this.callMetadataResolver = callMetadataResolver;
        this.reactorService = reactorService;
    }

    MethodDescriptor<REQ, RESP> getMethodDescriptor() {
        return binding.getMethodDescriptor();
    }

    void invoke(REQ request, StreamObserver<RESP> responseObserver) {
        Object[] args;
        if (binding.getCallMetadataPos() < 0) {
            args = binding.getGrpcArgumentPos() < 0 ? EMPTY_ARG_ARRAY : new Object[]{request};
        } else {
            CallMetadata callMetadata = callMetadataResolver.resolve().orElse(JobManagerConstants.UNDEFINED_CALL_METADATA);

            if (binding.getCallMetadataPos() == 0) {
                if (binding.getGrpcArgumentPos() < 0) {
                    args = new Object[]{callMetadata};
                } else {
                    args = new Object[]{callMetadata, request};
                }
            } else {
                if (binding.getCallMetadataPos() == 0) {
                    args = new Object[]{callMetadata, request};
                } else {
                    args = new Object[]{request, callMetadata};
                }
            }
        }

        Publisher<RESP> result;
        try {
            result = (Publisher<RESP>) binding.getReactorMethod().invoke(reactorService, args);
        } catch (InvocationTargetException e) {
            responseObserver.onError(e.getCause());
            return;
        } catch (Exception e) {
            responseObserver.onError(e);
            return;
        }

        Disposable disposable = handleResult(result, responseObserver);

        ((ServerCallStreamObserver) responseObserver).setOnCancelHandler(disposable::dispose);
    }

    abstract Disposable handleResult(Publisher<RESP> result, StreamObserver<RESP> responseObserver);
}
