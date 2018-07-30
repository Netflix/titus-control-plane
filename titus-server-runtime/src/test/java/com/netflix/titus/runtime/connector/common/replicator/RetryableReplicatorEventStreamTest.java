package com.netflix.titus.runtime.connector.common.replicator;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream.ReplicatorEvent;
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

public class RetryableReplicatorEventStreamTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final ReplicatorEventStream<String> delegate = mock(ReplicatorEventStream.class);

    private final RetryableReplicatorEventStream<String> jobStreamCache = new RetryableReplicatorEventStream<>(
            delegate, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, testScheduler
    );

    private final ExtTestSubscriber<ReplicatorEvent> cacheEventSubscriber = new ExtTestSubscriber<>();

    private PublishSubject<ReplicatorEvent<String>> eventSubject;

    @Before
    public void setUp() {
        when(delegate.connect()).thenAnswer(arg -> Observable.unsafeCreate(subscriber -> {
            eventSubject = PublishSubject.create();
            eventSubject.subscribe(subscriber);
        }));
    }

    @Test
    public void testImmediateConnect() {
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        eventSubject.onNext(new ReplicatorEvent<>("event1", 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);

        eventSubject.onNext(new ReplicatorEvent<>("event2", 2));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(2);
    }

    @Test
    public void testImmediateFailureWithSuccessfulReconnect() {
        jobStreamCache.connect().subscribe(cacheEventSubscriber);
        eventSubject.onError(new RuntimeException("simulated error"));

        testScheduler.advanceTimeBy(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        eventSubject.onNext(new ReplicatorEvent<>("event1", 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);
    }

    @Test
    public void testReconnectAfterFailure() {
        // First connect successfully, and return cache instance
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        eventSubject.onNext(new ReplicatorEvent<>("event1", 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);

        // Now fail
        eventSubject.onError(new RuntimeException("simulated error"));
        testScheduler.advanceTimeBy(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        // Recover
        eventSubject.onNext(new ReplicatorEvent<>("event2", 2));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(2);

        // Fail again
        eventSubject.onError(new RuntimeException("simulated error"));
        testScheduler.advanceTimeBy(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        // Recover again
        eventSubject.onNext(new ReplicatorEvent<>("event3", 3));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(3);
    }

    @Test
    public void testProlongedOutage() {
        // First connect successfully, and return cache instance
        jobStreamCache.connect().subscribe(cacheEventSubscriber);

        eventSubject.onNext(new ReplicatorEvent<>("event1", 1));
        assertThat(cacheEventSubscriber.takeNext().getLastUpdateTime()).isEqualTo(1);

        // Now fail
        eventSubject.onError(new RuntimeException("simulated error"));
        testScheduler.advanceTimeBy(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNull();

        //
        testScheduler.advanceTimeBy(RetryableReplicatorEventStream.LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNotNull();
    }
}