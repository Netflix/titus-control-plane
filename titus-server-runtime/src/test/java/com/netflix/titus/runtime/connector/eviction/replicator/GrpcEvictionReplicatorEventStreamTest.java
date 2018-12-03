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

package com.netflix.titus.runtime.connector.eviction.replicator;

import java.time.Duration;

import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.eviction.EvictionDataSnapshot;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.replicator.GrpcJobReplicatorEventStream;
import com.netflix.titus.testkit.model.eviction.EvictionComponentStub;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcEvictionReplicatorEventStreamTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobComponentStub JobComponentStub = new JobComponentStub(titusRuntime);

    private final EvictionComponentStub evictionComponentStub = new EvictionComponentStub(
            JobComponentStub,
            titusRuntime
    );

    private final EvictionServiceClient client = evictionComponentStub.getEvictionServiceClient();

    @Test
    public void testCacheBootstrap() {
        evictionComponentStub.setSystemQuota(25);
        evictionComponentStub.setJobQuota("job1", 5);

        newConnectVerifier()
                .assertNext(initialReplicatorEvent -> {
                    assertThat(initialReplicatorEvent).isNotNull();

                    EvictionDataSnapshot cache = initialReplicatorEvent.getSnapshot();
                    assertThat(cache.getSystemEvictionQuota().getQuota()).isEqualTo(25);
                    assertThat(cache.findJobEvictionQuota("job1").get().getQuota()).isEqualTo(5);
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testSystemQuotaUpdate() {
        evictionComponentStub.setSystemQuota(25);
        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().getSystemEvictionQuota().getQuota()).isEqualTo(25))
                .then(() -> evictionComponentStub.setSystemQuota(50))
                .thenConsumeWhile(next -> !isSystemQuotaUpdate(next, 50))
                .assertNext(next -> assertThat(next.getSnapshot().getSystemEvictionQuota().getQuota()).isEqualTo(50))
                .thenCancel()
                .verify();
    }

    @Test
    public void testJobQuotaUpdate() {
        evictionComponentStub.setJobQuota("job1", 1);
        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().findJobEvictionQuota("job1").get().getQuota()).isEqualTo(1))
                .then(() -> evictionComponentStub.setJobQuota("job1", 5))
                .thenConsumeWhile(next -> !isJobQuotaUpdate(next, "job1", 5))
                .assertNext(next -> assertThat(next.getSnapshot().findJobEvictionQuota("job1").get().getQuota()).isEqualTo(5))
                .thenCancel()
                .verify();
    }

    @Test
    public void testReEmit() {
        newConnectVerifier()
                .expectNextCount(1)
                .expectNoEvent(Duration.ofMillis(GrpcJobReplicatorEventStream.LATENCY_REPORT_INTERVAL_MS))
                .expectNextCount(1)

                .thenCancel()
                .verify();
    }

    private GrpcEvictionReplicatorEventStream newStream() {
        return new GrpcEvictionReplicatorEventStream(client, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, Schedulers.parallel());
    }

    private StepVerifier.FirstStep<ReplicatorEvent<EvictionDataSnapshot, EvictionEvent>> newConnectVerifier() {
        return StepVerifier.withVirtualTime(() -> newStream().connect().log());
    }

    private boolean isSystemQuotaUpdate(ReplicatorEvent<EvictionDataSnapshot, EvictionEvent> event, int expectedSystemQuota) {
        return event.getSnapshot().getSystemEvictionQuota().getQuota() == expectedSystemQuota;
    }

    private boolean isJobQuotaUpdate(ReplicatorEvent<EvictionDataSnapshot, EvictionEvent> event, String jobId, int expectedJobQuota) {
        return event.getSnapshot().findJobEvictionQuota(jobId).map(q -> q.getQuota() == expectedJobQuota).orElse(false);
    }
}