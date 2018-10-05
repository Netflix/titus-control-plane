package com.netflix.titus.common.util.rx;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ReactorHeadTransformerTest {

    private static final List<String> HEAD_ITEMS = asList("A", "B");

    private final AtomicBoolean terminateFlag = new AtomicBoolean();

    private final List<String> emittedTicks = new CopyOnWriteArrayList<>();

    private final Flux<String> hotObservable = Flux.interval(Duration.ofMillis(0), Duration.ofMillis(1), Schedulers.single())
            .map(tick -> {
                String value = "T" + tick;
                emittedTicks.add(value);
                return value;
            })
            .doOnTerminate(() -> terminateFlag.set(true))
            .doOnCancel(() -> terminateFlag.set(true));

    private final Flux<String> combined = hotObservable.compose(ReactorExt.head(() -> {
        while (emittedTicks.size() < 2) { // Tight loop
        }
        return HEAD_ITEMS;
    }));

    private final TitusRxSubscriber<String> testSubscriber = new TitusRxSubscriber<>();

    @After
    public void tearDown() {
        testSubscriber.dispose();
    }

    @Test
    public void testHotObservableBuffering() throws Exception {
        combined.subscribe(testSubscriber);

        // Wait until hot observable emits directly next item
        int currentTick = emittedTicks.size();
        while (emittedTicks.size() == currentTick && testSubscriber.isOpen()) {
        }
        testSubscriber.failIfClosed();

        assertThat(testSubscriber.isOpen()).isTrue();
        int checkedCount = HEAD_ITEMS.size() + currentTick + 1;
        List<String> checkedSequence = testSubscriber.takeNext(checkedCount, Duration.ofSeconds(5));

        testSubscriber.dispose();
        await().timeout(5, TimeUnit.SECONDS).until(terminateFlag::get);

        List<String> expected = new ArrayList<>(HEAD_ITEMS);
        expected.addAll(emittedTicks.subList(0, currentTick + 1));
        assertThat(checkedSequence).containsExactlyElementsOf(expected);
    }

    @Test
    public void testExceptionInSubscriberTerminatesSubscription() {
        AtomicInteger errorCounter = new AtomicInteger();
        Disposable subscription = combined.subscribe(
                next -> {
                    if (next.equals("T3")) {
                        throw new RuntimeException("simulated error");
                    }
                },
                e -> errorCounter.incrementAndGet()
        );

        // Wait until hot observable emits error
        while (!terminateFlag.get()) {
        }

        assertThat(terminateFlag.get()).isTrue();
        assertThat(subscription.isDisposed()).isTrue();
        assertThat(errorCounter.get()).isEqualTo(1);
    }

    @Test
    public void testExceptionInHead() {
        Flux.just("A").compose(ReactorExt.head(() -> {
            throw new RuntimeException("Simulated error");
        })).subscribe(testSubscriber);
        assertThat(testSubscriber.hasError()).isTrue();
    }
}