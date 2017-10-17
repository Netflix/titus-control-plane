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

package io.netflix.titus.gateway.service.v3.internal;


import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Emitter;
import rx.functions.Action0;
import rx.functions.Action1;

public class StreamObserverHelper {
    private static Logger log = LoggerFactory.getLogger(StreamObserverHelper.class);

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
}
