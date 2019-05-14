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

package com.netflix.titus.common.util.rx;

import java.time.Duration;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorReEmitterOperatorTest {

    private static final Duration EMIT_INTERVAL = Duration.ofMillis(1_000);

    private boolean cancelled;

    @Test
    public void testReEmits() {
        StepVerifier
                .withVirtualTime(() -> newReEmitter(EMIT_INTERVAL.multipliedBy(5), EMIT_INTERVAL.multipliedBy(2)))
                .expectSubscription()

                // Until first item is emitted, there is nothing to re-emit.
                .expectNoEvent(emitSteps(5))

                // First emit
                .expectNext("0")
                .expectNoEvent(EMIT_INTERVAL)
                .expectNext(toReEmitted("0"))

                // Second emit
                .expectNoEvent(EMIT_INTERVAL)
                .expectNext("1")
                .expectNoEvent(EMIT_INTERVAL)
                .expectNext(toReEmitted("1"))

                .thenCancel()
                .verify();

        assertThat(cancelled).isTrue();
    }

    @Test
    public void testNoReEmits() {
        Duration sourceInterval = EMIT_INTERVAL.dividedBy(2);

        StepVerifier
                .withVirtualTime(() -> newReEmitter(Duration.ofSeconds(0), sourceInterval))

                .expectNext("0")
                .expectNoEvent(sourceInterval)
                .expectNext("1")
                .expectNoEvent(sourceInterval)
                .expectNext("2")

                .thenCancel()
                .verify();
    }

    private Duration emitSteps(int steps) {
        return EMIT_INTERVAL.multipliedBy(steps);
    }

    private Flux<String> newReEmitter(Duration delay, Duration interval) {
        Flux<String> source = Flux.interval(delay, interval)
                .map(Object::toString)
                .doOnCancel(() -> this.cancelled = true);
        return source.compose(ReactorExt.reEmitter(this::toReEmitted, EMIT_INTERVAL, Schedulers.parallel()));
    }

    private String toReEmitted(String tick) {
        return "Re-emitted#" + tick;
    }
}