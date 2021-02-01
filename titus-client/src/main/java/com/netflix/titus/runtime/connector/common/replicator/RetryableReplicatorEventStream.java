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

package com.netflix.titus.runtime.connector.common.replicator;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

public class RetryableReplicatorEventStream<SNAPSHOT, TRIGGER> implements ReplicatorEventStream<SNAPSHOT, TRIGGER> {

    private static final Logger logger = LoggerFactory.getLogger(RetryableReplicatorEventStream.class);

    static final long INITIAL_RETRY_DELAY_MS = 500;
    static final long MAX_RETRY_DELAY_MS = 2_000;

    private final ReplicatorEventStream<SNAPSHOT, TRIGGER> delegate;
    private final DataReplicatorMetrics metrics;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public RetryableReplicatorEventStream(ReplicatorEventStream<SNAPSHOT, TRIGGER> delegate,
                                          DataReplicatorMetrics metrics,
                                          TitusRuntime titusRuntime,
                                          Scheduler scheduler) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> connect() {
        return newRetryableConnection().transformDeferred(
                ReactorExt.reEmitter(Function.identity(), Duration.ofMillis(LATENCY_REPORT_INTERVAL_MS), scheduler)
        );
    }

    private Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> newRetryableConnection() {
        Retry retryer = RetryHandlerBuilder.retryHandler()
                .withTitle("RetryableReplicatorEventStream of " + delegate.getClass().getSimpleName())
                .withUnlimitedRetries()
                .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                .withReactorScheduler(scheduler)
                .buildRetryExponentialBackoff();

        return delegate.connect()
                .doOnTerminate(metrics::disconnected)
                .retryWhen(retryer)
                .doOnNext(event -> {
                    metrics.connected();
                    metrics.event(event);
                })
                .doOnError(error -> {
                    // Because we always retry, we should never reach this point.
                    logger.warn("Retryable stream terminated with an error", new IllegalStateException(error)); // Record current stack trace if that happens
                    titusRuntime.getCodeInvariants().unexpectedError("Retryable stream terminated with an error", error.getMessage());
                    metrics.disconnected(error);
                })
                .doOnCancel(metrics::disconnected)
                .doOnTerminate(metrics::disconnected);
    }
}
