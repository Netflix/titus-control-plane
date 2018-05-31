package com.netflix.titus.runtime.connector.jobmanager.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import com.netflix.titus.runtime.connector.jobmanager.JobCacheResolver;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

@Singleton
public class DefaultJobCacheResolver implements JobCacheResolver {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobCacheResolver.class);

    // Staleness threshold checked during the system initialization.
    private static final long STALENESS_THRESHOLD = 60_000;

    private static final long INITIALIZATION_TIMEOUT_MS = 120_000;

    private final TitusRuntime titusRuntime;
    private final Subscription subscription;

    private final AtomicReference<JobStreamCache.CacheEvent> lastCacheEventRef = new AtomicReference<>();

    private final PublishSubject<Long> stalenessSubject = PublishSubject.create();
    private final Observable<Long> stalenessObservable = ObservableExt.protectFromMissingExceptionHandlers(stalenessSubject.asObservable(), logger);

    @Inject
    public DefaultJobCacheResolver(JobManagementClient client, TitusRuntime titusRuntime) {
        this(client, titusRuntime, Schedulers.computation());
    }

    @VisibleForTesting
    DefaultJobCacheResolver(JobManagementClient client, TitusRuntime titusRuntime, Scheduler scheduler) {
        this.titusRuntime = titusRuntime;

        CountDownLatch latch = new CountDownLatch(1);

        this.subscription = new RetryableJobStreamCache(
                new GrpcJobStreamCache(client, titusRuntime, scheduler),
                titusRuntime,
                scheduler
        ).connect().subscribe(
                next -> {
                    lastCacheEventRef.set(next);
                    if (latch.getCount() > 0 && isFresh(next)) {
                        latch.countDown();
                    }
                    stalenessSubject.onNext(next.getLastUpdateTime());
                },
                e -> logger.error("Unexpected error in the job cache event stream", e),
                () -> logger.info("Job cache event stream completed")
        );

        try {
            if (!latch.await(INITIALIZATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Job cache initialization timeout");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Cannot initialize the job cache", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }

    @Override
    public JobCache getCurrent() {
        return lastCacheEventRef.get().getCache();
    }

    @Override
    public long getStalenessMs() {
        return titusRuntime.getClock().wallTime() - lastCacheEventRef.get().getLastUpdateTime();
    }

    @Override
    public Observable<Long> observeDataStalenessMs() {
        return stalenessObservable;
    }

    private boolean isFresh(JobStreamCache.CacheEvent event) {
        long now = titusRuntime.getClock().wallTime();
        return event.getLastUpdateTime() + STALENESS_THRESHOLD >= now;
    }
}
