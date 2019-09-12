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

import io.grpc.stub.StreamObserver;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.ReplayProcessor;

import static org.assertj.core.api.Assertions.assertThat;

public class ServerStreamingMethodHandlerTest {

    private final StreamObserver<String> responseObserver = new StreamObserver<String>() {
        @Override
        public void onNext(String value) {
            throw new RuntimeException("Simulated error");
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    };

    @Test
    public void testOnNextExceptionHandlerOnSubscribe() {
        ReplayProcessor<String> publisher = ReplayProcessor.create(2);
        publisher.onNext("a");
        publisher.onNext("b");
        Disposable disposable = ServerStreamingMethodHandler.internalHandleResult(publisher, responseObserver);
        assertThat(disposable.isDisposed()).isTrue();
    }

    @Test
    public void testOnNextExceptionHandlerAfterSubscribe() {
        DirectProcessor<String> publisher = DirectProcessor.create();
        Disposable disposable = ServerStreamingMethodHandler.internalHandleResult(publisher, responseObserver);

        publisher.onNext("a");
        publisher.onNext("b");
        assertThat(disposable.isDisposed()).isTrue();
    }
}