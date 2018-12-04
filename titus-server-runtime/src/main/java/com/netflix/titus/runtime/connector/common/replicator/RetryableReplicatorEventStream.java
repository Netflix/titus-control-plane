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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public class RetryableReplicatorEventStream<SNAPSHOT, TRIGGER> implements ReplicatorEventStream<SNAPSHOT, TRIGGER> {

    private static final Logger logger = LoggerFactory.getLogger(RetryableReplicatorEventStream.class);

    static final long INITIAL_RETRY_DELAY_MS = 500;
    static final long MAX_RETRY_DELAY_MS = 2_000;

    private final ReplicatorEvent<SNAPSHOT, TRIGGER> initialEvent;
    private final ReplicatorEventStream<SNAPSHOT, TRIGGER> delegate;
    private final DataReplicatorMetrics metrics;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public RetryableReplicatorEventStream(SNAPSHOT initialSnapshot,
                                          TRIGGER initialTrigger,
                                          ReplicatorEventStream<SNAPSHOT, TRIGGER> delegate,
                                          DataReplicatorMetrics metrics,
                                          TitusRuntime titusRuntime,
                                          Scheduler scheduler) {
        this.initialEvent = new ReplicatorEvent<>(initialSnapshot, initialTrigger, 0);
        this.delegate = delegate;
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> connect() {
        return connectInternal(initialEvent);
    }

    private Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> connectInternal(ReplicatorEvent<SNAPSHOT, TRIGGER> lastReplicatorEvent) {
        return createDelegateEmittingAtLeastOneItem(lastReplicatorEvent)
                .onErrorResume(e -> {
                    metrics.disconnected();

                    if (e instanceof DataReplicatorException) {
                        DataReplicatorException cacheException = (DataReplicatorException) e;
                        if (cacheException.getLastCacheEvent().isPresent()) {
                            logger.info("Reconnecting after error: {}", e.getMessage());
                            logger.debug("Stack trace", e);
                            return connectInternal((ReplicatorEvent<SNAPSHOT, TRIGGER>) cacheException.getLastCacheEvent().get());
                        }
                    }

                    // We expect to get DataReplicatorException always. If this is not the case, we reconnect with empty cache.
                    titusRuntime.getCodeInvariants().unexpectedError("Expected DataReplicatorException exception with the latest cache instance", e);
                    return connect();
                })
                .doOnNext(event -> {
                    metrics.connected();
                    metrics.event(titusRuntime.getClock().wallTime() - event.getLastUpdateTime());
                })
                .doOnCancel(metrics::disconnected)
                .doOnError(error -> {
                    // Because we always retry, we should never reach this point.
                    logger.warn("Retryable stream terminated with an error", new IllegalStateException(error)); // Record current stack trace if that happens
                    titusRuntime.getCodeInvariants().unexpectedError("Retryable stream terminated with an error", error.getMessage());
                    metrics.disconnected(error);
                })
                .doOnComplete(metrics::disconnected);
    }

    private Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> createDelegateEmittingAtLeastOneItem(ReplicatorEvent<SNAPSHOT, TRIGGER> lastReplicatorEvent) {
        return Flux.defer(() -> {
                    AtomicReference<ReplicatorEvent<SNAPSHOT, TRIGGER>> ref = new AtomicReference<>(lastReplicatorEvent);

                    Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> staleCacheObservable = Flux
                            .interval(
                                    Duration.ofMillis(LATENCY_REPORT_INTERVAL_MS),
                                    Duration.ofMillis(LATENCY_REPORT_INTERVAL_MS),
                                    scheduler
                            )
                            .takeUntil(tick -> ref.get() != lastReplicatorEvent)
                            .map(tick -> lastReplicatorEvent);

                    Function<Flux<Throwable>, Publisher<?>> retryer = RetryHandlerBuilder.retryHandler()
                            .withRetryWhen(() -> ref.get() == lastReplicatorEvent)
                            .withUnlimitedRetries()
                            .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                            .withReactorScheduler(scheduler)
                            .buildReactorExponentialBackoff();

                    Flux<ReplicatorEvent<SNAPSHOT, TRIGGER>> newCacheObservable = delegate.connect()
                            .doOnNext(ref::set)
                            .retryWhen(retryer)
                            .onErrorResume(e -> Flux.error(new DataReplicatorException(Optional.ofNullable(ref.get()), e)));

                    return Flux.merge(staleCacheObservable, newCacheObservable);
                }
        );
    }
}
