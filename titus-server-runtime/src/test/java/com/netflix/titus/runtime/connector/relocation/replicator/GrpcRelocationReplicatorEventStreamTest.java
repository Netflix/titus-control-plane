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

package com.netflix.titus.runtime.connector.relocation.replicator;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.relocation.RelocationServiceClient;
import com.netflix.titus.runtime.connector.relocation.TaskRelocationSnapshot;
import com.netflix.titus.testkit.model.relocation.RelocationComponentStub;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcRelocationReplicatorEventStreamTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final RelocationComponentStub relocationConnectorStubs = new RelocationComponentStub(titusRuntime);

    private final RelocationServiceClient client = relocationConnectorStubs.getClient();

    @Test
    public void testCacheBootstrap() {
        relocationConnectorStubs.addPlan(TaskRelocationPlan.newBuilder().withTaskId("task1").build());

        newConnectVerifier()
                .assertNext(snapshotEvent -> {
                    assertThat(snapshotEvent.getSnapshot().getPlans()).containsKey("task1");
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testPlanAddAndRemove() {
        newConnectVerifier()
                .assertNext(snapshotEvent -> {
                    assertThat(snapshotEvent.getSnapshot().getPlans()).isEmpty();
                })
                .then(() ->
                        relocationConnectorStubs.addPlan(TaskRelocationPlan.newBuilder().withTaskId("task1").build())
                )
                .assertNext(snapshotEvent -> {
                    assertThat(snapshotEvent.getSnapshot().getPlans()).containsKey("task1");
                })
                .then(() -> {
                    relocationConnectorStubs.removePlan("task1");
                })

                .thenCancel()
                .verify();
    }

    private GrpcRelocationReplicatorEventStream newStream() {
        return new GrpcRelocationReplicatorEventStream(client, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, Schedulers.parallel());
    }

    private StepVerifier.FirstStep<ReplicatorEvent<TaskRelocationSnapshot, TaskRelocationEvent>> newConnectVerifier() {
        return StepVerifier.withVirtualTime(() -> newStream().connect().log());
    }
}