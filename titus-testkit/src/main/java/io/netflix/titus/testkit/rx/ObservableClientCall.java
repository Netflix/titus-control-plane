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

package io.netflix.titus.testkit.rx;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netflix.titus.testkit.grpc.GrpcClientErrorUtils;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.observers.SerializedSubscriber;

import static io.netflix.titus.runtime.endpoint.v3.grpc.ErrorResponses.KEY_TITUS_DEBUG;

/**
 */
public class ObservableClientCall<ReqT, RespT> extends ClientCall.Listener<RespT> {

    private final Subscriber<? super RespT> subscriber;
    private final Subscription inputSubscription;

    private ObservableClientCall(ClientCall<ReqT, RespT> clientCall, Observable<ReqT> input, Subscriber<? super RespT> subscriber) {
        this.subscriber = new SerializedSubscriber<>(subscriber);
        subscriber.add(new Subscription() {
            @Override
            public void unsubscribe() {
                clientCall.cancel("Subscriber terminated the stream subscription", new OnTerminateException());
            }

            @Override
            public boolean isUnsubscribed() {
                return false;
            }
        });
        Metadata headers = new Metadata();
        headers.put(KEY_TITUS_DEBUG, "true");
        clientCall.start(this, headers);

        subscriber.setProducer(n -> {
            int safeN = n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
            if (safeN > 0) {
                clientCall.request(safeN);
            }
        });

        // Input
        this.inputSubscription = input.subscribe(
                clientCall::sendMessage,
                e -> {
                    clientCall.cancel("Request input stream terminated with an error", e);
                    subscriber.onError(e);
                },
                clientCall::halfClose
        );
    }

    @Override
    public void onMessage(RespT message) {
        subscriber.onNext(message);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
        if (status.equals(Status.OK)) {
            subscriber.onCompleted();
        } else {
            StatusRuntimeException statusException = status.asRuntimeException(trailers);
            GrpcClientErrorUtils.printDetails(statusException);
            subscriber.onError(statusException);
        }
        inputSubscription.unsubscribe();
    }

    public static <ReqT, RespT> Observable<RespT> create(ClientCall<ReqT, RespT> clientCall, Observable<ReqT> input) {
        return Observable.create(subscriber -> new ObservableClientCall<>(clientCall, input, subscriber));
    }

    public static <ReqT, RespT> Observable<RespT> create(ClientCall<ReqT, RespT> clientCall, ReqT input) {
        return create(clientCall, Observable.just(input));
    }

    private static class OnTerminateException extends Throwable {
    }
}
