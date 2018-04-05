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

package com.netflix.titus.ext.aws;

import java.util.concurrent.Future;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import rx.Completable;
import rx.CompletableSubscriber;
import rx.Single;
import rx.SingleSubscriber;
import rx.exceptions.Exceptions;
import rx.functions.Action1;
import rx.functions.Action2;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

public class AwsObservableExt {
    /**
     * Wraps usage of async clients from the AWS SDK into a rx.Completable. The action is expected to produce a single
     * Future, and the returned Completable will propagate its termination events (error and success) to subscribers.
     * <p>
     * Subscription cancellations on the returned Completable will cancel the Future if it is still pending.
     * <p>
     * Usage:
     * <p>
     * <pre>
     * Completable c = AwsObservableExt.asyncActionCompletable(supplier -> client.someAsyncOperation(request, supplier.handler(
     *         (req, resp) -> doSomethingOnSuccess(req, resp),
     *         (t) -> doSomethingOnError(t)
     * )));
     * </pre>
     *
     * @param action that will be executed when the Completable is subscribed to.
     * @return a Completable that completes when the produced future completes
     */
    public static Completable asyncActionCompletable(Func1<CompletableHandlerSupplier, Future<?>> action) {
        return Completable.create(subscriber -> {
            try {
                final Future<?> result = action.call(new CompletableHandlerSupplier(subscriber));
                subscriber.onSubscribe(Subscriptions.create(() -> result.cancel(true)));
            } catch (Throwable t) {
                Exceptions.throwIfFatal(t);
                subscriber.onError(t);
            }
        });
    }

    /**
     * Wraps usage of async clients from the AWS SDK into a rx.Single. The action is expected to produce a single
     * Future, and the returned Single will propagate its result (error and success) to subscribers.
     * <p>
     * Subscription cancellations on the returned Single will cancel the Future if it is still pending.
     * <p>
     * The provided <tt>action</tt> will run on the AWS SDK threadpool. If desired, execution can be brought back to a
     * different scheduler with {@link rx.Observable#observeOn}.
     * <p>
     * Usage:
     * <pre>
     * {@code
     * Single s = AwsObservableExt.asyncActionSingle(supplier -> client.someAsyncOperation(request, supplier.handler()))
     *     .observeOn(Schedulers.computation());
     * }
     * </pre>
     *
     * @param action that will be executed when the Single is subscribed to, and produces a Future
     * @param <RES>  return type in the Future produced by the action
     * @return a Single that propagates the result of the produced future
     */
    public static <REQ extends AmazonWebServiceRequest, RES> Single<RES> asyncActionSingle(Func1<SingleHandlerSupplier<RES>, Future<RES>> action) {
        return Single.create(subscriber -> {
            try {
                final Future<RES> result = action.call(new SingleHandlerSupplier<>(subscriber));
                subscriber.add(Subscriptions.create(() -> result.cancel(true)));
            } catch (Throwable t) {
                Exceptions.throwIfFatal(t);
                subscriber.onError(t);
            }
        });
    }

    public static class CompletableHandlerSupplier {
        private final CompletableSubscriber subscriber;

        private CompletableHandlerSupplier(CompletableSubscriber subscriber) {
            this.subscriber = subscriber;
        }

        public <REQ extends AmazonWebServiceRequest, RES> AsyncHandler<REQ, RES> handler(Action2<REQ, RES> onSuccessAction, Action1<Exception> onErrorAction) {
            return new AsyncHandler<REQ, RES>() {
                @Override
                public void onError(Exception exception) {
                    onErrorAction.call(exception);
                    subscriber.onError(exception);
                }

                @Override
                public void onSuccess(REQ request, RES result) {
                    onSuccessAction.call(request, result);
                    subscriber.onCompleted();
                }
            };
        }
    }

    public static class SingleHandlerSupplier<RES> {
        private final SingleSubscriber<? super RES> subscriber;

        private SingleHandlerSupplier(SingleSubscriber<? super RES> subscriber) {
            this.subscriber = subscriber;
        }

        public <REQ extends AmazonWebServiceRequest> AsyncHandler<REQ, RES> handler() {
            return new AsyncHandler<REQ, RES>() {
                @Override
                public void onError(Exception exception) {
                    subscriber.onError(exception);
                }

                @Override
                public void onSuccess(REQ request, RES result) {
                    subscriber.onSuccess(result);
                }
            };
        }
    }
}
