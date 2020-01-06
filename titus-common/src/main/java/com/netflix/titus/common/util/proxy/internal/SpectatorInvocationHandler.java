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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Subscription;

import static java.util.Arrays.asList;

/**
 * Method invocation metrics collector.
 */
public class SpectatorInvocationHandler<API, NATIVE> extends InterceptingInvocationHandler<API, NATIVE, Long> {

    private static final String INVOCATION_COUNTER_METRIC_NAME = "titusMaster.api.invocation.count";
    private static final String INVOCATION_TIME_METRIC_NAME = "titusMaster.api.invocation.executionTime";
    private static final String RESULT_SUBSCRIPTION_COUNT_METRIC_NAME = "titusMaster.api.invocation.subscriptions";
    private static final String RESULT_SUBSCRIPTION_EMITS_METRIC_NAME = "titusMaster.api.invocation.subscriptionEmits";
    private static final String RESULT_SUBSCRIPTION_TIME_METRIC_NAME = "titusMaster.api.invocation.subscriptionTime";

    private static final Tag TAG_STATUS_SUCCESS = new BasicTag("status", "success");
    private static final Tag TAG_STATUS_ERROR = new BasicTag("status", "error");

    private static final Tag TAG_CALL_STAGE_ON_METHOD_EXIT = new BasicTag("callStage", "onMethodExit");
    private static final Tag TAG_CALL_STAGE_ON_COMPLETED = new BasicTag("callStage", "onCompleted");
    private static final Tag TAG_CALL_STAGE_ON_MONO_SUCCESS = new BasicTag("callStage", "onSuccess");

    private final Registry registry;
    private final Clock clock;

    private final List<Tag> commonTags;

    public SpectatorInvocationHandler(String instanceName, Class<API> apiInterface, TitusRuntime titusRuntime, boolean followObservableResults) {
        super(apiInterface, followObservableResults);
        this.registry = titusRuntime.getRegistry();
        this.clock = titusRuntime.getClock();
        this.commonTags = asList(
                new BasicTag("instance", instanceName),
                new BasicTag("class", apiInterface.getName())
        );
    }

    @Override
    protected Long before(Method method, Object[] args) {
        return clock.wallTime();
    }

    @Override
    protected void after(Method method, Object result, Long startTime) {
        registry.counter(
                INVOCATION_COUNTER_METRIC_NAME,
                tags(
                        "method", method.getName(),
                        "status", "success"
                )
        ).increment();

        reportExecutionTime(method, startTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_METHOD_EXIT);

        if (!isAsynchronous(result)) {
            reportExecutionTime(method, startTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);
        }
    }

    @Override
    protected void afterException(Method method, Throwable error, Long startTime) {
        registry.counter(
                INVOCATION_COUNTER_METRIC_NAME,
                tags(
                        "method", method.getName(),
                        "status", "error",
                        "exception", getExceptionName(error)
                )
        ).increment();

        reportExecutionTime(method, startTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_METHOD_EXIT);
    }

    @Override
    protected Observable<Object> afterObservable(Method method, Observable<Object> result, Long startTime) {
        long methodExitTime = clock.wallTime();

        return Observable.unsafeCreate(subscriber -> {
            long subscriptionTime = clock.wallTime();

            registry.counter(
                    RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                    tags(
                            "method", method.getName(),
                            "subscriptionStage", "subscribed"
                    )
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, subscriptionTime);

            Subscription subscription = result.doOnUnsubscribe(() -> {
                registry.counter(
                        RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                        tags(
                                "method", method.getName(),
                                "subscriptionStage", "unsubscribed"
                        )
                ).increment();
            }).subscribe(
                    next -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_EMITS_METRIC_NAME,
                                tags("method", method.getName())
                        ).increment();
                        subscriber.onNext(next);
                    },
                    error -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                tags(
                                        "method", method.getName(),
                                        "subscriptionStage", "onError",
                                        "exception", getExceptionName(error)
                                )
                        ).increment();
                        reportExecutionTime(method, subscriptionTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_COMPLETED);

                        subscriber.onError(error);
                    },
                    () -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                tags(
                                        "method", method.getName(),
                                        "subscriptionStage", "onCompleted"
                                )
                        ).increment();
                        reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);

                        subscriber.onCompleted();
                    }
            );

            subscriber.add(subscription);
        });
    }

    @Override
    protected Flux<Object> afterFlux(Method method, Flux<Object> result, Long startTime) {
        long methodExitTime = clock.wallTime();

        return Flux.create(emitter -> {
            long subscriptionTime = clock.wallTime();

            registry.counter(
                    RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                    tags(
                            "method", method.getName(),
                            "subscriptionStage", "subscribed"
                    )
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, subscriptionTime);

            Disposable subscription = result.doOnCancel(() -> {
                registry.counter(
                        RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                        tags(
                                "method", method.getName(),
                                "subscriptionStage", "unsubscribed"
                        )
                ).increment();
            }).subscribe(
                    next -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_EMITS_METRIC_NAME,
                                tags("method", method.getName())
                        ).increment();
                        emitter.next(next);
                    },
                    error -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                tags(
                                        "method", method.getName(),
                                        "subscriptionStage", "onError",
                                        "exception", getExceptionName(error)
                                )
                        ).increment();
                        reportExecutionTime(method, subscriptionTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_COMPLETED);

                        emitter.error(error);
                    },
                    () -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                tags(
                                        "method", method.getName(),
                                        "subscriptionStage", "onCompleted"
                                )
                        ).increment();
                        reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);

                        emitter.complete();
                    }
            );

            emitter.onCancel(subscription);
        });
    }

    @Override
    protected Completable afterCompletable(Method method, Completable result, Long aLong) {
        long methodExitTime = clock.wallTime();

        return Completable.create(subscriber -> {
            long subscriptionTime = clock.wallTime();

            registry.counter(
                    RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                    tags(
                            "method", method.getName(),
                            "subscriptionStage", "subscribed"
                    )
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, subscriptionTime);

            Subscription subscription = result
                    .doOnUnsubscribe(() -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                tags(
                                        "method", method.getName(),
                                        "subscriptionStage", "unsubscribed"
                                )
                        ).increment();
                    }).subscribe(
                            () -> {
                                registry.counter(
                                        RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                        tags(
                                                "method", method.getName(),
                                                "subscriptionStage", "onCompleted"
                                        )
                                ).increment();
                                reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_COMPLETED);

                                subscriber.onCompleted();
                            },
                            error -> {
                                registry.counter(
                                        RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                        tags(
                                                "method", method.getName(),
                                                "subscriptionStage", "onError",
                                                "exception", getExceptionName(error)
                                        )
                                ).increment();
                                reportExecutionTime(method, subscriptionTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_COMPLETED);

                                subscriber.onError(error);
                            }
                    );

            subscriber.onSubscribe(subscription);
        });
    }

    @Override
    protected Mono<Object> afterMono(Method method, Mono<Object> result, Long aLong) {
        long methodExitTime = clock.wallTime();

        return Mono.create(sink -> {
            long subscriptionTime = clock.wallTime();

            registry.counter(
                    RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                    tags(
                            "method", method.getName(),
                            "subscriptionStage", "subscribed"
                    )
            ).increment();
            reportSubscriptionExecutionTime(method, methodExitTime, subscriptionTime);

            AtomicBoolean emittedValue = new AtomicBoolean();
            Disposable subscription = result
                    .doOnCancel(() -> {
                        registry.counter(
                                RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                tags(
                                        "method", method.getName(),
                                        "subscriptionStage", "unsubscribed"
                                )
                        ).increment();
                    }).subscribe(
                            next -> {
                                emittedValue.set(true);
                                registry.counter(
                                        RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                        tags(
                                                "method", method.getName(),
                                                "subscriptionStage", "onSuccess",
                                                "monoWithValue", "true"
                                        )
                                ).increment();
                                reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_MONO_SUCCESS);

                                sink.success(next);
                            },
                            error -> {
                                registry.counter(
                                        RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                        tags(
                                                "method", method.getName(),
                                                "subscriptionStage", "onError",
                                                "exception", getExceptionName(error)
                                        )
                                ).increment();
                                reportExecutionTime(method, subscriptionTime, TAG_STATUS_ERROR, TAG_CALL_STAGE_ON_MONO_SUCCESS);

                                sink.error(error);
                            },
                            () -> {
                                if (!emittedValue.get()) {
                                    registry.counter(
                                            RESULT_SUBSCRIPTION_COUNT_METRIC_NAME,
                                            tags(
                                                    "method", method.getName(),
                                                    "subscriptionStage", "onSuccess",
                                                    "monoWithValue", "false"
                                            )
                                    ).increment();
                                    reportExecutionTime(method, subscriptionTime, TAG_STATUS_SUCCESS, TAG_CALL_STAGE_ON_MONO_SUCCESS);

                                    sink.success();
                                }
                            }
                    );

            sink.onCancel(subscription);
        });
    }

    private void reportExecutionTime(Method method, Long startTime, Tag... tags) {
        Id id = registry.createId(INVOCATION_TIME_METRIC_NAME,
                tags("method", method.getName())
        ).withTags(tags);
        registry.timer(id).record(clock.wallTime() - startTime, TimeUnit.MILLISECONDS);
    }

    private void reportSubscriptionExecutionTime(Method method, Long startTime, long endTime) {
        registry.timer(
                RESULT_SUBSCRIPTION_TIME_METRIC_NAME,
                tags("method", method.getName())
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

    private List<Tag> tags(String... values) {
        List<Tag> result = new ArrayList<>(commonTags.size() + values.length / 2);
        result.addAll(commonTags);
        for (int i = 0; i < values.length / 2; i++) {
            result.add(new BasicTag(values[i * 2], values[i * 2 + 1]));
        }
        return result;
    }
}
