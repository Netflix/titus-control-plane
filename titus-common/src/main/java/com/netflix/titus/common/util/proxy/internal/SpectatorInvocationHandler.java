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

package com.netflix.titus.common.util.proxy.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import rx.Completable;
import rx.Observable;
import rx.Subscription;

/**
 * Method invocation metrics collector.
 */
public class SpectatorInvocationHandler<API, NATIVE> extends InterceptingInvocationHandler<API, NATIVE, Long> {

    private static final Tag TAG_STATUS_SUCCESS = new BasicTag("status", "success");
    private static final Tag TAG_STATUS_ERROR = new BasicTag("status", "error");

    private static final Tag TAG_CALL_STAGE_ON_METHOD_EXIT = new BasicTag("callStage", "onMethodExit");
    private static final Tag TAG_CALL_STAGE_ON_COMPLETED = new BasicTag("callStage", "onCompleted");

    private final Class<API> apiInterface;
    private final Registry registry;
    private final String invocationCounterMetricName;
    private final String invocationTimeMetricName;
    private final String resultSubscriptionCountMetricName;
    private final String resultSubscriptionEmitMetricName;
    private final String resultSubscriptionTimeMetricName;

    public SpectatorInvocationHandler(Class<API> apiInterface, Registry registry, boolean followObservableResults) {
        super(apiInterface, followObservableResults);
        this.apiInterface = apiInterface;
        this.registry = registry;

        this.invocationCounterMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".invocations";
        this.invocationTimeMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".executionTime";
        this.resultSubscriptionCountMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".subscriptions";
        this.resultSubscriptionEmitMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".subscriptionEmits";
        this.resultSubscriptionTimeMetricName = "titusMaster.api." + apiInterface.getSimpleName() + ".subscriptionTime";
    }

    @Override
    protected Long before(Method method, Object[] args) {
        return System.currentTimeMillis();
    }

    @Override
    protected void after(Method method, Object result, Long startTime) {
        registry.counter(
                invocationCounterMetricName,
                "class", apiInterface.getName(),
                "method", method.getName(),
                "status", "success"
        ).increment();

        reportExecutionTime(method, startTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_METHOD_EXIT);

        if (!isAsynchronous(result)) {
            reportExecutionTime(method, startTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);
        }
    }

    @Override
    protected void afterException(Method method, Throwable error, Long startTime) {
        registry.counter(
                invocationCounterMetricName,
                "class", apiInterface.getName(),
                "method", method.getName(),
                "status", "error",
                "exception", getExceptionName(error)
        ).increment();

        reportExecutionTime(method, startTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_METHOD_EXIT);
    }

    @Override
    protected Observable<Object> afterObservable(Method method, Observable<Object> result, Long startTime) {
        long methodExitTime = System.currentTimeMillis();

        return Observable.unsafeCreate(subscriber -> {
            long subscriptionTime = System.currentTimeMillis();

            registry.counter(
                    resultSubscriptionCountMetricName,
                    "class", apiInterface.getName(),
                    "method", method.getName(),
                    "subscriptionStage", "subscribed"
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, subscriptionTime);

            Subscription subscription = result.doOnUnsubscribe(() -> {
                registry.counter(
                        resultSubscriptionCountMetricName,
                        "class", apiInterface.getName(),
                        "method", method.getName(),
                        "subscriptionStage", "unsubscribed"
                ).increment();
            }).subscribe(
                    next -> {
                        registry.counter(
                                resultSubscriptionEmitMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName()
                        ).increment();
                        subscriber.onNext(next);
                    },
                    error -> {
                        registry.counter(
                                resultSubscriptionCountMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName(),
                                "subscriptionStage", "onError",
                                "exception", getExceptionName(error)
                        ).increment();
                        reportExecutionTime(method, subscriptionTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_COMPLETED);

                        subscriber.onError(error);
                    },
                    () -> {
                        registry.counter(
                                resultSubscriptionCountMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName(),
                                "subscriptionStage", "onCompleted"
                        ).increment();
                        reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);

                        subscriber.onCompleted();
                    }
            );

            subscriber.add(subscription);
        });
    }

    @Override
    protected Completable afterCompletable(Method method, Completable result, Long aLong) {
        long methodExitTime = System.currentTimeMillis();

        return Completable.create(subscriber -> {
            long subscriptionTime = System.currentTimeMillis();

            registry.counter(
                    resultSubscriptionCountMetricName,
                    "class", apiInterface.getName(),
                    "method", method.getName(),
                    "subscriptionStage", "subscribed"
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, subscriptionTime);

            Subscription subscription = result
                    .doOnUnsubscribe(() -> {
                        registry.counter(
                                resultSubscriptionCountMetricName,
                                "class", apiInterface.getName(),
                                "method", method.getName(),
                                "subscriptionStage", "unsubscribed"
                        ).increment();
                    }).subscribe(
                            () -> {
                                registry.counter(
                                        resultSubscriptionCountMetricName,
                                        "class", apiInterface.getName(),
                                        "method", method.getName(),
                                        "subscriptionStage", "onCompleted"
                                ).increment();
                                reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);

                                subscriber.onCompleted();
                            },
                            error -> {
                                registry.counter(
                                        resultSubscriptionCountMetricName,
                                        "class", apiInterface.getName(),
                                        "method", method.getName(),
                                        "subscriptionStage", "onError",
                                        "exception", getExceptionName(error)
                                ).increment();
                                reportExecutionTime(method, subscriptionTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_COMPLETED);

                                subscriber.onError(error);
                            }
                    );

            subscriber.onSubscribe(subscription);
        });
    }

    private void reportExecutionTime(Method method, Long startTime, Tag... tags) {
        Id id = registry.createId(invocationTimeMetricName,
                "class", apiInterface.getName(),
                "method", method.getName()
        ).withTags(tags);
        registry.timer(id).record(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    private void reportSubscriptionExecutionTime(Method method, Long startTime, long endTime) {
        registry.timer(
                resultSubscriptionTimeMetricName,
                "class", apiInterface.getName(),
                "method", method.getName()
        ).record(endTime - startTime, TimeUnit.MILLISECONDS);
    }

    private boolean isAsynchronous(Object result) {
        return result instanceof Observable || result instanceof Completable;
    }

    private String getExceptionName(Throwable error) {
        return error instanceof InvocationHandler
                ? error.getCause().getClass().getName()
                : error.getClass().getName();
    }
}
