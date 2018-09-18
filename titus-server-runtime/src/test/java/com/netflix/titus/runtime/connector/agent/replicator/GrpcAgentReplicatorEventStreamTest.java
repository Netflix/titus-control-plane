package com.netflix.titus.runtime.connector.agent.replicator;

import java.time.Duration;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.connector.agent.AgentManagementClient;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.common.replicator.DataReplicatorMetrics;
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream.ReplicatorEvent;
import com.netflix.titus.runtime.connector.jobmanager.replicator.GrpcJobReplicatorEventStream;
import com.netflix.titus.testkit.model.agent.AgentDeployment;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcAgentReplicatorEventStreamTest {

    private static final int FLEX_1_DESIRED = 5;
    private static final int CRITICAL_1_DESIRED = 10;

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final AgentDeployment deployment = AgentDeployment.newDeployment()
            .withActiveInstanceGroup(Tier.Flex, "flex-1", AwsInstanceType.R4_4XLarge, FLEX_1_DESIRED)
            .withActiveInstanceGroup(Tier.Critical, "critical-1", AwsInstanceType.M4_4XLarge, CRITICAL_1_DESIRED)
            .build();

    private final AgentManagementClient client = mock(AgentManagementClient.class);

    @Test
    public void testCacheBootstrap() {
        newConnectVerifier().
                assertNext(initialReplicatorEvent -> {
                    assertThat(initialReplicatorEvent).isNotNull();
                    assertThat(initialReplicatorEvent.getData().getInstanceGroups()).hasSize(2);
                    assertThat(initialReplicatorEvent.getData().getInstances()).hasSize(FLEX_1_DESIRED + CRITICAL_1_DESIRED);
                })

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceGroupUpdate() {
        newConnectVerifier()
                .assertNext(next -> assertThat(next.getData().findInstanceGroup("flex-1").get().getTier()).isEqualTo(Tier.Flex))
                .then(() -> deployment.changeTier("flex-1", Tier.Critical))
                .assertNext(next -> assertThat(next.getData().findInstanceGroup("flex-1").get().getTier()).isEqualTo(Tier.Critical))

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceGroupRemove() {
        newConnectVerifier()
                .then(() -> deployment.removeInstanceGroup("flex-1"))
                .expectNextCount(FLEX_1_DESIRED - 1)
                .assertNext(next -> assertThat(next.getData().getInstances("flex-1")).hasSize(1))
                .assertNext(next -> assertThat(next.getData().getInstances("flex-1")).isEmpty())

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceUpdate() {
        AgentInstance instance = deployment.getFirstInstance();
        InstanceLifecycleStatus newStatus = InstanceLifecycleStatus.newBuilder()
                .withState(InstanceLifecycleState.Stopped)
                .build();

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getData().findInstance(instance.getId()).get().getLifecycleStatus()).isEqualTo(instance.getLifecycleStatus()))
                .then(() -> deployment.changeInstanceLifecycleStatus(instance.getId(), newStatus))
                .assertNext(next -> assertThat(next.getData().findInstance(instance.getId()).get().getLifecycleStatus()).isEqualTo(newStatus))

                .thenCancel()
                .verify();
    }

    @Test
    public void testCacheInstanceRemove() {
        AgentInstance instance = deployment.getFirstInstance();

        newConnectVerifier()
                .assertNext(next -> assertThat(next.getData().findInstance(instance.getId())).isPresent())
                .then(() -> deployment.removeInstance(instance.getId()))
                .assertNext(next -> assertThat(next.getData().findInstance(instance.getId())).isNotPresent())

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
        when(client.observeAgents()).thenReturn(deployment.grpcObserveAgents(true));
        return new GrpcAgentReplicatorEventStream(client, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, Schedulers.parallel());
    }

    private StepVerifier.FirstStep<ReplicatorEvent<AgentSnapshot>> newConnectVerifier() {
        return StepVerifier.withVirtualTime(() -> newStream().connect().log());
    }
}