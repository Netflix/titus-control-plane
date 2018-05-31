package com.netflix.titus.runtime.connector.jobmanager.cache;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryableJobStreamCacheTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final JobStreamCache delegate = mock(JobStreamCache.class);

    private final RetryableJobStreamCache jobStreamCache = new RetryableJobStreamCache(delegate, titusRuntime, testScheduler);

    private final ExtTestSubscriber<JobStreamCache.CacheEvent> cacheEventSubscriber = new ExtTestSubscriber<>();

    private PublishSubject<JobStreamCache.CacheEvent> cacheEventSubject;

    @Before
    public void setUp() {
        when(delegate.connect()).thenAnswer(arg -> Observable.unsafeCreate(subscriber -> {
            cacheEventSubject = PublishSubject.create();
            cacheEventSubject.subscribe(subscriber);
        }));
    }

    @Test
    public void testImmediateConnect() {
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        cacheEventSubject.onNext(new JobStreamCache.CacheEvent(JobCache.empty(), 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);

        cacheEventSubject.onNext(new JobStreamCache.CacheEvent(JobCache.empty(), 2));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(2);
    }

    @Test
    public void testImmediateFailureWithSuccessfulReconnect() {
        jobStreamCache.connect().subscribe(cacheEventSubscriber);
        cacheEventSubject.onError(new RuntimeException("simulated error"));

        testScheduler.advanceTimeBy(RetryableJobStreamCache.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        cacheEventSubject.onNext(new JobStreamCache.CacheEvent(JobCache.empty(), 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);
    }

    @Test
    public void testReconnectAfterFailure() {
        // First connect successfully, and return cache instance
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        cacheEventSubject.onNext(new JobStreamCache.CacheEvent(JobCache.empty(), 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);

        // Now fail
        cacheEventSubject.onError(new RuntimeException("simulated error"));
        testScheduler.advanceTimeBy(RetryableJobStreamCache.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        // Recover again
        cacheEventSubject.onNext(new JobStreamCache.CacheEvent(JobCache.empty(), 2));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(2);
    }

    @Test
    public void testProlongedOutage() {
        // First connect successfully, and return cache instance
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        cacheEventSubject.onNext(new JobStreamCache.CacheEvent(JobCache.empty(), 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);

        // Now fail
        cacheEventSubject.onError(new RuntimeException("simulated error"));
        testScheduler.advanceTimeBy(RetryableJobStreamCache.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        //
        testScheduler.advanceTimeBy(RetryableJobStreamCache.LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNotNull();
    }
}