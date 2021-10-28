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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamDataReplicatorTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final ReplicatorEventStreamStub replicatorEventStream = new ReplicatorEventStreamStub();

    private final DataReplicatorMetrics metrics = new DataReplicatorMetrics("test", false, titusRuntime);

    private final Sinks.Many<ReplicatorEvent<StringSnapshot, String>> eventSink = Sinks.many().multicast().directAllOrNothing();

    @Test
    public void testBootstrap() {
        StepVerifier
                .withVirtualTime(() -> StreamDataReplicator.newStreamDataReplicator(replicatorEventStream, false, metrics, titusRuntime))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))

                .then(() -> eventSink.emitNext(new ReplicatorEvent<>(new StringSnapshot("firstUpdate"), "firstTrigger", 0), Sinks.EmitFailureHandler.FAIL_FAST))
                .assertNext(replicator -> {
                    assertThat(replicator.getCurrent()).isEqualTo(new StringSnapshot("firstUpdate"));
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testShutdown() {
        AtomicReference<StreamDataReplicator> replicatorRef = new AtomicReference<>();
        StepVerifier
                .withVirtualTime(() ->
                        StreamDataReplicator.newStreamDataReplicator(replicatorEventStream, false, metrics, titusRuntime)
                                .flatMap(replicator -> {
                                    replicatorRef.set(replicator);
                                    return replicator.events();
                                })
                )
                .expectSubscription()
                .then(() -> eventSink.emitNext(new ReplicatorEvent<>(new StringSnapshot("firstUpdate"), "firstTrigger", 0), Sinks.EmitFailureHandler.FAIL_FAST))

                .then(() -> {
                    assertThat(eventSink.currentSubscriberCount()).isEqualTo(1);
                    replicatorRef.get().close();
                    assertThat(eventSink.currentSubscriberCount()).isZero();

                })
                .verifyErrorMatches(error -> error.getMessage().equals("Data replicator closed"));
    }

    private static class StringSnapshot extends ReplicatedSnapshot {

        private final String value;

        private StringSnapshot(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StringSnapshot that = (StringSnapshot) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toSummaryString() {
            return value;
        }
    }

    private class ReplicatorEventStreamStub implements ReplicatorEventStream<StringSnapshot, String> {

        @Override
        public Flux<ReplicatorEvent<StringSnapshot, String>> connect() {
            return eventSink.asFlux();
        }
    }
}