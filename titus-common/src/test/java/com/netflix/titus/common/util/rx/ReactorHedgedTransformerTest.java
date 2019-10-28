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

    private static final List<Duration> THRESHOLDS = asList(Duration.ofMillis(1), Duration.ofMillis(10), Duration.ofMillis(100));

    private static final Map<String, String> CONTEXT = Collections.singletonMap("test", "123");

    @Test
    public void testSuccess() {
        testSuccess(0, new boolean[]{true, false, false, false});
        testSuccess(1, new boolean[]{false, true, false, false});
        testSuccess(10, new boolean[]{false, false, true, false});
        testSuccess(100, new boolean[]{false, false, false, true});
    }

    @Test
    public void testConcurrent() {
        StepVerifier.withVirtualTime(() -> newSource(false, false, false, true)
                .compose(hedgeOf(asList(Duration.ZERO, Duration.ZERO, Duration.ZERO)))
        )
                .thenAwait(Duration.ZERO)
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    public void testAllFailures() {
        StepVerifier.withVirtualTime(() -> newSource(false, false, false, false).compose(hedgeOf(THRESHOLDS)))
                .thenAwait(Duration.ofMillis(100))
                .expectErrorMatches(error -> error.getMessage().contains("4 exceptions occurred"))
                .verify();
    }

    private void testSuccess(long awaitMs, boolean[] successFailurePattern) {
        StepVerifier.withVirtualTime(() -> newSource(successFailurePattern).compose(hedgeOf(THRESHOLDS)))
                .thenAwait(Duration.ofMillis(awaitMs))
                .expectNext(1L)
                .verifyComplete();
    }

    private Mono<Long> newSource(boolean... successFailureFlags) {
        AtomicInteger indexRef = new AtomicInteger();
        return Mono.defer(() ->
                Mono.fromCallable(() -> {
                    int index = indexRef.getAndIncrement() % successFailureFlags.length;
                    if (!successFailureFlags[index]) {
                        throw new RuntimeException("simulated error");
                    }
                    return 1L;
                })
        );
    }

    private Function<Mono<Long>, Mono<Long>> hedgeOf(List<Duration> thresholds) {
        return ReactorExt.hedged(
                thresholds,
                CONTEXT,
                new DefaultRegistry(),
                Schedulers.parallel()
        );
    }
}