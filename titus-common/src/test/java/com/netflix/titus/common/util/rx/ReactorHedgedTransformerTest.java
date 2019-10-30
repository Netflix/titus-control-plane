/*
 * Copyright 2019 Netflix, Inc.
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.netflix.spectator.api.DefaultRegistry;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static java.util.Arrays.asList;

public class ReactorHedgedTransformerTest {

    enum Behavior {
        Success,
        RetryableError,
        NotRetryableError
    }

    private static final RuntimeException SIMULATED_RETRYABLE_ERROR = new RuntimeException("simulated retryable error");

    private static final RuntimeException SIMULATED_NOT_RETRYABLE_ERROR = new RuntimeException("simulated not retryable error");

    private static final List<Duration> THRESHOLDS = asList(Duration.ofMillis(1), Duration.ofMillis(10), Duration.ofMillis(100));

    private static final Map<String, String> CONTEXT = Collections.singletonMap("test", "123");

    @Test
    public void testSuccess() {
        testSuccess(0, new Behavior[]{Behavior.Success, Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError});
        testSuccess(1, new Behavior[]{Behavior.RetryableError, Behavior.Success, Behavior.RetryableError, Behavior.RetryableError});
        testSuccess(10, new Behavior[]{Behavior.RetryableError, Behavior.RetryableError, Behavior.Success, Behavior.RetryableError});
        testSuccess(100, new Behavior[]{Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError, Behavior.Success});
    }

    private void testSuccess(long awaitMs, Behavior[] behaviors) {
        StepVerifier.withVirtualTime(() -> newSource(behaviors).compose(hedgeOf(THRESHOLDS)))
                .thenAwait(Duration.ofMillis(awaitMs))
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    public void testMonoVoidSuccess() {
        StepVerifier.withVirtualTime(() ->
                newVoidSource(new Behavior[]{Behavior.RetryableError, Behavior.Success, Behavior.RetryableError, Behavior.RetryableError})
                        .compose(hedgeOf(THRESHOLDS))
        )
                .thenAwait(Duration.ofMillis(1))
                .expectComplete()
                .verify();
    }

    @Test
    public void testFailureWithNonRetryableError() {
        StepVerifier.withVirtualTime(() ->
                newSource(new Behavior[]{Behavior.NotRetryableError, Behavior.Success, Behavior.Success, Behavior.Success})
                        .compose(hedgeOf(THRESHOLDS))
        )
                .thenAwait(Duration.ofMillis(0))
                .expectErrorMatches(error -> error == SIMULATED_NOT_RETRYABLE_ERROR)
                .verify();
    }

    @Test
    public void testConcurrent() {
        StepVerifier.withVirtualTime(() ->
                newSource(Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError, Behavior.Success)
                        .compose(hedgeOf(asList(Duration.ZERO, Duration.ZERO, Duration.ZERO)))
        )
                .thenAwait(Duration.ZERO)
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    public void testAllFailures() {
        StepVerifier.withVirtualTime(() ->
                newSource(Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError)
                        .compose(hedgeOf(THRESHOLDS))
        )
                .thenAwait(Duration.ofMillis(100))
                .expectErrorMatches(error -> error == SIMULATED_RETRYABLE_ERROR)
                .verify();
    }

    @Test
    public void testAllMonoVoidFailures() {
        StepVerifier.withVirtualTime(() ->
                newVoidSource(Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError, Behavior.RetryableError)
                        .compose(hedgeOf(THRESHOLDS))
        )
                .thenAwait(Duration.ofMillis(100))
                .expectErrorMatches(error -> error == SIMULATED_RETRYABLE_ERROR)
                .verify();
    }

    private Mono<Long> newSource(Behavior... behaviors) {
        return newSource(1L, behaviors);
    }

    private Mono<Void> newVoidSource(Behavior... behaviors) {
        return newSource(null, behaviors);
    }

    private <T> Mono<T> newSource(T value, Behavior[] behaviors) {
        AtomicInteger indexRef = new AtomicInteger();
        return Mono.defer(() ->
                Mono.fromCallable(() -> {
                    int index = indexRef.getAndIncrement() % behaviors.length;
                    switch (behaviors[index]) {
                        case RetryableError:
                            throw SIMULATED_RETRYABLE_ERROR;
                        case NotRetryableError:
                            throw SIMULATED_NOT_RETRYABLE_ERROR;
                        case Success:
                    }
                    return value;
                })
        );
    }

    private <T> Function<Mono<T>, Mono<T>> hedgeOf(List<Duration> thresholds) {
        return ReactorExt.hedged(
                thresholds,
                error -> error == SIMULATED_RETRYABLE_ERROR,
                CONTEXT,
                new DefaultRegistry(),
                Schedulers.parallel()
        );
    }
}