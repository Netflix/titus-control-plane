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

package com.netflix.titus.runtime.connector.common.replicator;

import java.time.Duration;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.junit.Test;
import org.reactivestreams.Processor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryableReplicatorEventStreamTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final ReplicatorEventStream<String, String> delegate = mock(ReplicatorEventStream.class);

    private Processor<ReplicatorEvent<String, String>, ReplicatorEvent<String, String>> eventSubject;

    @Test
    public void testImmediateConnect() {
        newConnectVerifier()
                // Event 1
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("event1", "trigger1", 1)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(1))

                // Event 2
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("event2", "trigger2", 2)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(2))

                .thenCancel()
                .verify();
    }

    @Test
    public void testImmediateFailureWithSuccessfulReconnect() {
        newConnectVerifier()
                // Simulated error
                .then(() -> eventSubject.onError(new RuntimeException("simulated error")))
                .expectNoEvent(Duration.ofMillis(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS))

                // Event 1
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("event1", "trigger1", 1)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(1))

                .thenCancel()
                .verify();
    }

    @Test
    public void testReconnectAfterFailure() {
        newConnectVerifier()
                // Event 1
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("event1", "trigger1", 1)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(1))

                // Now fail
                .then(() -> eventSubject.onError(new RuntimeException("simulated error")))
                .expectNoEvent(Duration.ofMillis(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS))

                // Recover
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("even2", "trigger2", 2)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(2))

                // Fail again
                .then(() -> eventSubject.onError(new RuntimeException("simulated error")))
                .expectNoEvent(Duration.ofMillis(RetryableReplicatorEventStream.INITIAL_RETRY_DELAY_MS * 2))

                // Recover again
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("even3", "trigger3", 3)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(3))

                .thenCancel()
                .verify();
    }

    @Test
    public void testInProlongedOutageTheLastKnownItemIsReEmitted() {
        newConnectVerifier()
                // Event 1
                .then(() -> eventSubject.onNext(new ReplicatorEvent<>("event1", "trigger1", 1)))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(1))

                // Now fail
                .then(() -> eventSubject.onError(new RuntimeException("simulated error")))

                // Expect re-emit
                .expectNoEvent(Duration.ofMillis(RetryableReplicatorEventStream.LATENCY_REPORT_INTERVAL_MS))
                .assertNext(e -> assertThat(e.getLastUpdateTime()).isEqualTo(1))

                .thenCancel()
                .verify();
    }

    private RetryableReplicatorEventStream<String, String> newStream() {
        when(delegate.connect()).thenAnswer(invocation -> Flux.defer(() -> {
            eventSubject = EmitterProcessor.create();
            return eventSubject;
        }));

        return new RetryableReplicatorEventStream<>(
                delegate, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, Schedulers.parallel()
        );
    }

    private StepVerifier.FirstStep<ReplicatorEvent<String, String>> newConnectVerifier() {
        return StepVerifier.withVirtualTime(() -> newStream().connect().log());
    }
}