package com.netflix.titus.common.util.rx;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class ReactorReEmitterOperatorTest {

    private static final long INTERVAL_MS = 1_000;

    private final DirectProcessor<String> subject = DirectProcessor.create();

    @Test
    public void testReEmits() {
        StepVerifier
                .withVirtualTime(this::newReEmitter)
                .expectSubscription()

                // Until first item is emitted, there is nothing to re-emit.
                .expectNoEvent(advanceSteps(2))

                // Emit 'A' and wait for re-emit
                .then(() -> subject.onNext("A"))
                .expectNext("A")

                .expectNoEvent(advanceSteps(1))
                .expectNext("a")

                // Emit 'B' and wait for re-emit
                .then(() -> subject.onNext("B"))
                .expectNext("B")

                .expectNoEvent(advanceSteps(1))
                .expectNext("b")

                .thenCancel()
                .verify();
    }

    @Test
    public void testNoReEmits() {
        StepVerifier
                .withVirtualTime(this::newReEmitter)

                .then(() -> subject.onNext("A"))
                .expectNext("A")
                .expectNoEvent(Duration.ofMillis(INTERVAL_MS - 1))

                .then(() -> subject.onNext("B"))
                .expectNext("B")
                .expectNoEvent(Duration.ofMillis(INTERVAL_MS - 1))

                .thenCancel()
                .verify();
    }

    private Duration advanceSteps(int steps) {
        return Duration.ofMillis(steps * INTERVAL_MS);
    }

    private Flux<String> newReEmitter() {
        return subject.compose(ReactorExt.reEmitter(String::toLowerCase, INTERVAL_MS, TimeUnit.MILLISECONDS, Schedulers.parallel()));
    }
}