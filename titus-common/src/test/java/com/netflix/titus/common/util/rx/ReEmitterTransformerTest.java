package com.netflix.titus.common.util.rx;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;

public class ReEmitterTransformerTest {

    private static final long INTERVAL_MS = 1_000;

    public final TestScheduler testScheduler = Schedulers.test();

    private final PublishSubject<String> subject = PublishSubject.create();

    private final ExtTestSubscriber<String> subscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() {
        subject.compose(ObservableExt.reemiter(String::toLowerCase, INTERVAL_MS, TimeUnit.MILLISECONDS, testScheduler)).subscribe(subscriber);
    }

    @Test
    public void testReemits() {
        // Until first item is emitted, there is nothing to re-emit.
        advanceSteps(2);
        assertThat(subscriber.takeNext()).isNull();

        // Emit 'A' and wait for re-emit
        subject.onNext("A");
        assertThat(subscriber.takeNext()).isEqualTo("A");

        advanceSteps(1);
        assertThat(subscriber.takeNext()).isEqualTo("a");

        // Emit 'B' and wait for re-emit
        subject.onNext("B");
        assertThat(subscriber.takeNext()).isEqualTo("B");

        advanceSteps(1);
        assertThat(subscriber.takeNext()).isEqualTo("b");
    }

    @Test
    public void testNoReemits() {
        subject.onNext("A");
        assertThat(subscriber.takeNext()).isEqualTo("A");

        testScheduler.advanceTimeBy(INTERVAL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(subscriber.takeNext()).isNull();

        subject.onNext("B");
        assertThat(subscriber.takeNext()).isEqualTo("B");

        testScheduler.advanceTimeBy(INTERVAL_MS - 1, TimeUnit.MILLISECONDS);
        assertThat(subscriber.takeNext()).isNull();
    }

    private void advanceSteps(int steps) {
        testScheduler.advanceTimeBy(steps * INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
}