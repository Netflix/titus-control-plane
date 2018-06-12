package com.netflix.titus.runtime.connector.agent.replicator;

import java.util.concurrent.TimeUnit;

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
import com.netflix.titus.runtime.connector.common.replicator.ReplicatorEventStream;
import com.netflix.titus.runtime.connector.jobmanager.replicator.GrpcJobReplicatorEventStream;
import com.netflix.titus.testkit.model.agent.AgentDeployment;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GrpcAgentReplicatorEventStreamTest {

    private static final int FLEX_1_DESIRED = 5;
    private static final int CRITICAL_1_DESIRED = 10;

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final AgentDeployment deployment = AgentDeployment.newDeployment()
            .withActiveInstanceGroup(Tier.Flex, "flex-1", AwsInstanceType.R4_4XLarge, FLEX_1_DESIRED)
            .withActiveInstanceGroup(Tier.Critical, "critical-1", AwsInstanceType.M4_4XLarge, CRITICAL_1_DESIRED)
            .build();

    private final AgentManagementClient client = mock(AgentManagementClient.class);

    private final GrpcAgentReplicatorEventStream agentStreamCache = new GrpcAgentReplicatorEventStream(client, new DataReplicatorMetrics("test", titusRuntime), titusRuntime, testScheduler);

    private final ExtTestSubscriber<ReplicatorEventStream.ReplicatorEvent<AgentSnapshot>> cacheEventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() {
        when(client.observeAgents()).thenReturn(deployment.grpcObserveAgents(true));
    }

    @Test
    public void testCacheBootstrap() {
        agentStreamCache.connect().subscribe(cacheEventSubscriber);

        ReplicatorEventStream.ReplicatorEvent<AgentSnapshot> initialReplicatorEvent = cacheEventSubscriber.takeNext();
        assertThat(initialReplicatorEvent).isNotNull();
        assertThat(initialReplicatorEvent.getData().getInstanceGroups()).hasSize(2);
        assertThat(initialReplicatorEvent.getData().getInstances()).hasSize(FLEX_1_DESIRED + CRITICAL_1_DESIRED);
    }

    @Test
    public void testCacheInstanceGroupUpdate() {
        bootstrap();
        deployment.changeTier("flex-1", Tier.Critical);
        assertThat(cacheEventSubscriber.takeNext().getData().findInstanceGroup("flex-1").get().getTier()).isEqualTo(Tier.Critical);
    }

    @Test
    public void testCacheInstanceGroupRemove() {
        bootstrap();

        deployment.removeInstanceGroup("flex-1");
        for (int i = 0; i < FLEX_1_DESIRED; i++) {
            assertThat(cacheEventSubscriber.takeNext().getData().getInstances("flex-1")).hasSize(FLEX_1_DESIRED - i - 1);
        }
        assertThat(cacheEventSubscriber.takeNext().getData().findInstanceGroup("flex-1")).isNotPresent();
    }

    @Test
    public void testCacheInstanceUpdate() {
        bootstrap();

        AgentInstance instance = deployment.getFirstInstance();
        InstanceLifecycleStatus newStatus = InstanceLifecycleStatus.newBuilder()
                .withState(InstanceLifecycleState.Stopped)
                .build();

        deployment.changeInstanceLifecycleStatus(instance.getId(), newStatus);
        assertThat(cacheEventSubscriber.takeNext().getData().findInstance(instance.getId()).get().getLifecycleStatus()).isEqualTo(newStatus);
    }

    @Test
    public void testCacheInstanceRemove() {
        bootstrap();
        AgentInstance instance = deployment.getFirstInstance();
        deployment.removeInstance(instance.getId());
        assertThat(cacheEventSubscriber.takeNext().getData().findInstance(instance.getId())).isNotPresent();
    }

    @Test
    public void testReemit() {
        bootstrap();
        assertThat(cacheEventSubscriber.takeNext()).isNull();
        testScheduler.advanceTimeBy(GrpcJobReplicatorEventStream.LATENCY_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cacheEventSubscriber.takeNext()).isNotNull();
    }

    private void bootstrap() {
        agentStreamCache.connect().subscribe(cacheEventSubscriber);
        cacheEventSubscriber.skipAvailable();
    }
}