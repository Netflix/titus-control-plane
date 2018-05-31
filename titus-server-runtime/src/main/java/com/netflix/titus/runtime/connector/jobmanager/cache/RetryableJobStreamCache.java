package com.netflix.titus.runtime.connector.jobmanager.cache;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;

public class RetryableJobStreamCache implements JobStreamCache {

    private static final CacheEvent UNINITIALIZED = new CacheEvent(JobCache.empty(), 0);

    static final long INITIAL_RETRY_DELAY_MS = 500;
    static final long MAX_RETRY_DELAY_MS = 2_000;

    private final JobStreamCache delegate;
    private final TitusRuntime titusRuntime;
    private final Scheduler scheduler;

    public RetryableJobStreamCache(JobStreamCache delegate, TitusRuntime titusRuntime, Scheduler scheduler) {
        this.delegate = delegate;
        this.titusRuntime = titusRuntime;
        this.scheduler = scheduler;
    }

    @Override
    public Observable<CacheEvent> connect() {
        return createDelegateEmittingAtLeastOneItem(UNINITIALIZED)
                .onErrorResumeNext(e -> {
                    if (e instanceof JobStreamCacheException) {
                        JobStreamCacheException cacheException = (JobStreamCacheException) e;
                        if (cacheException.getLastCacheEvent().isPresent()) {
                            return createDelegateEmittingAtLeastOneItem(cacheException.getLastCacheEvent().get());
                        }
                    }

                    // We expect to get JobStreamCacheException always. If this is not the case, we reconnect with empty cache.
                    titusRuntime.getCodeInvariants().unexpectedError("Expected JobStreamCacheException exception with the latest cache instance", e);
                    return connect();
                });
    }

    private Observable<CacheEvent> createDelegateEmittingAtLeastOneItem(CacheEvent lastCacheEvent) {
        return Observable.fromCallable(() -> new AtomicReference<>(lastCacheEvent))
                .flatMap(ref -> {
                            Observable<CacheEvent> staleCacheObservable = Observable.interval(LATENCY_REPORT_INTERVAL_MS, LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler)
                                    .takeUntil(tick -> ref.get() != lastCacheEvent)
                                    .map(tick -> {
                                        return lastCacheEvent;
                                    });

                            Func1<Observable<? extends Throwable>, Observable<?>> retryer = RetryHandlerBuilder.retryHandler()
                                    .withRetryWhen(() -> ref.get() == lastCacheEvent)
                                    .withUnlimitedRetries()
                                    .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                                    .withScheduler(scheduler)
                                    .buildExponentialBackoff();

                            Observable<CacheEvent> newCacheObservable = delegate.connect()
                                    .doOnNext(ref::set)
                                    .retryWhen(retryer)
                                    .onErrorResumeNext(e -> Observable.error(new JobStreamCacheException(Optional.ofNullable(ref.get()), e)));

                            return Observable.merge(staleCacheObservable, newCacheObservable);
                        }
                );
    }
}
