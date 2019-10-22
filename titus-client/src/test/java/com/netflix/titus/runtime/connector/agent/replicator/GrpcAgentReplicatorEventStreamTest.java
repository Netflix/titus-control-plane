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

package com.netflix.titus.runtime.connector.agent.replicator;

import java.time.Duration;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEvent;
import com.netflix.titus.runtime.connector.jobmanager.replicator.GrpcJobReplicatorEventStream;
import com.netflix.titus.testkit.model.agent.AgentComponentStub;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static com.netflix.titus.testkit.model.agent.AgentComponentStub.newAgentComponent;
import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentServerGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcAgentReplicatorEventStreamTest {

    private static final int FLEX_1_DESIRED = 5;
    private static final int CRITICAL_1_DESIRED = 10;

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final AgentComponentStub agentComponentStub = newAgentComponent()
            .addInstanceGroup(agentServerGroup("flex-1", Tier.Flex, FLEX_1_DESIRED, AwsInstanceType.R4_4XLarge))
            .addInstanceGroup(agentServerGroup("critical-1", Tier.Critical, CRITICAL_1_DESIRED, AwsInstanceType.M4_4XLarge));

    private final AgentManagementClient client = mock(AgentManagementClient.class);

    @Test
    public void testCacheBootstrap() {
        newConnectVerifier().
                assertNext(initialReplicatorEvent -> {
                    assertThat(initialReplicatorEvent).isNotNull();
                    assertThat(initialReplicatorEvent.getSnapshot().getInstanceGroups()).hasSize(2);
                    assertThat(initialReplicatorEvent.getSnapshot().getInstances()).hasSize(FLEX_1_DESIRED + CRITICAL_1_DESIRED);
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceGroupUpdate() {
        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().findInstanceGroup("flex-1").get().getTier()).isEqualTo(Tier.Flex))
                .then(() -> agentComponentStub.changeTier("flex-1", Tier.Critical))
                .assertNext(next -> assertThat(next.getSnapshot().findInstanceGroup("flex-1").get().getTier()).isEqualTo(Tier.Critical))

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceGroupRemove() {
        newConnectVerifier()
                .then(() -> agentComponentStub.removeInstanceGroup("flex-1"))
                .expectNextCount(FLEX_1_DESIRED - 1)
                .assertNext(next -> assertThat(next.getSnapshot().getInstances("flex-1")).hasSize(1))
                .assertNext(next -> assertThat(next.getSnapshot().getInstances("flex-1")).isEmpty())

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceUpdate() {
        AgentInstance instance = agentComponentStub.getAgentManagementService().getAgentInstances("flex-1").get(0);
        InstanceLifecycleStatus newStatus = InstanceLifecycleStatus.newBuilder()
                .withState(InstanceLifecycleState.Stopped)
                .build();

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().findInstance(instance.getId()).get().getLifecycleStatus()).isEqualTo(instance.getLifecycleStatus()))
                .then(() -> agentComponentStub.changeInstanceLifecycleStatus(instance.getId(), newStatus))
                .assertNext(next -> {
                    assertThat(next.getSnapshot().findInstance(instance.getId()).get().getLifecycleStatus()).isEqualTo(newStatus);
                    assertThat(next.getSnapshot().getInstances(instance.getInstanceGroupId())).hasSize(FLEX_1_DESIRED);
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceRemove() {
        AgentInstance instance = agentComponentStub.getFirstInstance();

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getSnapshot().findInstance(instance.getId())).isPresent())
                .then(() -> agentComponentStub.terminateInstance(instance.getId(), true))
                .assertNext(next -> assertThat(next.getSnapshot().findInstance(instance.getId())).isNotPresent())

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

    private GrpcAgentReplicatorEventStream newStream() {
        when(client.observeAgents()).thenReturn(agentComponentStub.grpcObserveAgents(true));
        return new GrpcAgentReplicatorEventStream(client, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, Schedulers.parallel());
    }

    private StepVerifier.FirstStep<ReplicatorEvent<AgentSnapshot, AgentEvent>> newConnectVerifier() {
        return StepVerifier.withVirtualTime(() -> newStream().connect().log());
    }
}