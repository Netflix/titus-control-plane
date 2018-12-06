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
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamDataReplicatorTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final ReplicatorEventStreamStub replicatorEventStream = new ReplicatorEventStreamStub();

    private final DataReplicatorMetrics metrics = new DataReplicatorMetrics("test", titusRuntime);

    private final DirectProcessor<ReplicatorEvent<String, String>> eventPublisher = DirectProcessor.create();

    @Test
    public void testBootstrap() {
        StepVerifier
                .withVirtualTime(() -> StreamDataReplicator.newStreamDataReplicator(replicatorEventStream, metrics, titusRuntime))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))

                .then(() -> eventPublisher.onNext(new ReplicatorEvent<>("firstUpdate", "firstTrigger", 0)))
                .assertNext(replicator -> {
                    assertThat(replicator.getCurrent()).isEqualTo("firstUpdate");
                })

                .thenCancel()
                .verify();
    }

    private class ReplicatorEventStreamStub implements ReplicatorEventStream<String, String> {

        @Override
        public Flux<ReplicatorEvent<String, String>> connect() {
            return eventPublisher;
        }
    }
}