package com.netflix.titus.runtime.connector.common.replicator;

import java.time.Duration;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream.ReplicatorEvent;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamDataReplicatorTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final ReplicatorEventStreamStub replicatorEventStream = new ReplicatorEventStreamStub();

    private final DataReplicatorMetrics metrics = new DataReplicatorMetrics("test", titusRuntime);

    private final DirectProcessor<ReplicatorEvent<String>> eventPublisher = DirectProcessor.create();

    @Test
    public void testBootstrap() {
        StepVerifier
                .withVirtualTime(() -> StreamDataReplicator.newStreamDataReplicator(replicatorEventStream, metrics, titusRuntime))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))

                .then(() -> eventPublisher.onNext(new ReplicatorEvent<>("firstUpdate", 0)))
                .assertNext(replicator -> {
                    assertThat(replicator.getCurrent()).isEqualTo("firstUpdate");
                })
                
                .thenCancel()
                .verify();
    }

    private class ReplicatorEventStreamStub implements ReplicatorEventStream<String> {

        @Override
        public Flux<ReplicatorEvent<String>> connect() {
            return eventPublisher;
        }
    }
}