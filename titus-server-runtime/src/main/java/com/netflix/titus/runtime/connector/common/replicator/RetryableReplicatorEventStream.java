package com.netflix.titus.runtime.connector.common.replicator;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;

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
    public Observable<ReplicatorEvent<D>> connect() {
        return connectInternal((ReplicatorEvent<D>) UNINITIALIZED);
    }

    private Observable<ReplicatorEvent<D>> connectInternal(ReplicatorEvent<D> lastReplicatorEvent) {
        return createDelegateEmittingAtLeastOneItem(lastReplicatorEvent)
                .onErrorResumeNext(e -> {
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
                .doOnUnsubscribe(metrics::disconnected)
                .doOnError(error -> {
                    // Because we always retry, we should never reach this point.
                    logger.warn("Retryable stream terminated with an error", new IllegalStateException(error)); // Record current stack trace if that happens
                    titusRuntime.getCodeInvariants().unexpectedError("Retryable stream terminated with an error", error.getMessage());
                    metrics.disconnected(error);
                })
                .doOnCompleted(metrics::disconnected);
    }

    private Observable<ReplicatorEvent<D>> createDelegateEmittingAtLeastOneItem(ReplicatorEvent<D> lastReplicatorEvent) {
        return Observable.fromCallable(() -> new AtomicReference<>(lastReplicatorEvent))
                .flatMap(ref -> {
                            Observable<ReplicatorEvent<D>> staleCacheObservable = Observable.interval(LATENCY_REPORT_INTERVAL_MS, LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler)
                                    .takeUntil(tick -> ref.get() != lastReplicatorEvent)
                                    .map(tick -> lastReplicatorEvent);

                            Func1<Observable<? extends Throwable>, Observable<?>> retryer = RetryHandlerBuilder.retryHandler()
                                    .withRetryWhen(() -> ref.get() == lastReplicatorEvent)
                                    .withUnlimitedRetries()
                                    .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                                    .withScheduler(scheduler)
                                    .buildExponentialBackoff();

                            Observable<ReplicatorEvent<D>> newCacheObservable = delegate.connect()
                                    .doOnNext(ref::set)
                                    .retryWhen(retryer)
                                    .onErrorResumeNext(e -> Observable.error(new DataReplicatorException(Optional.ofNullable(ref.get()), e)));

                            return Observable.merge(staleCacheObservable, newCacheObservable);
                        }
                );
    }
}
