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

public class RetryableReplicatorEventStream<D> implements ReplicatorEventStream<D> {

    private static final Logger logger = LoggerFactory.getLogger(RetryableReplicatorEventStream.class);

    private static final ReplicatorEvent<Object> UNINITIALIZED = new ReplicatorEvent<>(new Object(), 0);

    static final long INITIAL_RETRY_DELAY_MS = 500;
    static final long MAX_RETRY_DELAY_MS = 2_000;

    private final ReplicatorEventStream<D> delegate;
    private final DataReplicatorMetrics metrics;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public RetryableReplicatorEventStream(ReplicatorEventStream<D> delegate,
                                          DataReplicatorMetrics metrics,
                                          TitusRuntime titusRuntime,
                                          Scheduler scheduler) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<ReplicatorEvent<D>> connect() {
        return connectInternal((ReplicatorEvent<D>) UNINITIALIZED);
    }

    private Flux<ReplicatorEvent<D>> connectInternal(ReplicatorEvent<D> lastReplicatorEvent) {
        return createDelegateEmittingAtLeastOneItem(lastReplicatorEvent)
                .onErrorResume(e -> {
                    metrics.disconnected();

                    if (e instanceof DataReplicatorException) {
                        DataReplicatorException cacheException = (DataReplicatorException) e;
                        if (cacheException.getLastCacheEvent().isPresent()) {
                            logger.info("Reconnecting after error: {}", e.getMessage());
                            logger.debug("Stack trace", e);
                            return connectInternal((ReplicatorEvent<D>) cacheException.getLastCacheEvent().get());
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

    private Flux<ReplicatorEvent<D>> createDelegateEmittingAtLeastOneItem(ReplicatorEvent<D> lastReplicatorEvent) {
        return Flux.defer(() -> {
                    AtomicReference<ReplicatorEvent<D>> ref = new AtomicReference<>(lastReplicatorEvent);

                    Flux<ReplicatorEvent<D>> staleCacheObservable = Flux
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

                    Flux<ReplicatorEvent<D>> newCacheObservable = delegate.connect()
                            .doOnNext(ref::set)
                            .retryWhen(retryer)
                            .onErrorResume(e -> Flux.error(new DataReplicatorException(Optional.ofNullable(ref.get()), e)));

                    return Flux.merge(staleCacheObservable, newCacheObservable);
                }
        );
    }
}
