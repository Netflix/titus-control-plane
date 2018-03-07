package io.netflix.titus.testkit.embedded.cloud.agent.player;


import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.resource.ComputeResources;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class ContainerPlayersManagerTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final ContainerPlayersManager playersManager = new ContainerPlayersManager(new DefaultRegistry(), testScheduler);

    private final SimulatedTitusAgent agent = new SimulatedTitusAgent(
            "testCluster",
            new ComputeResources(),
            "myHost",
            Protos.SlaveID.newBuilder().setValue("agentId").build(),
            Protos.Offer.newBuilder()
                    .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("testFramework"))
                    .setHostname("myHost"),
            AwsInstanceType.M4_4XLarge,
            16,
            0,
            65536,
            128_000,
            2_000,
            8,
            playersManager,
            testScheduler
    );

    private final ExtTestSubscriber<Protos.TaskStatus> statusObserver = new ExtTestSubscriber<>();

    private TaskExecutorHolder taskHolder;

    @Test
    public void testPositiveScenario() {
        initTaskHolder(ImmutableMap.of(
                TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX, "0",
                "TASK_LIFECYCLE_1", "selector: slots=0.. slotStep=2; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s"
        ));
        assertThat(playersManager.play(taskHolder)).isTrue();

        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_STAGING);

        testScheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_STARTING);

        testScheduler.advanceTimeBy(3, TimeUnit.SECONDS);
        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_RUNNING);

        testScheduler.advanceTimeBy(61, TimeUnit.SECONDS);
        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_FINISHED);
    }

    @Test
    public void testFailure() {
        initTaskHolder(ImmutableMap.of(
                TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX, "0",
                "TASK_LIFECYCLE_1", "selector: slots=0.. slotStep=2; launched: delay=2s; startInitiated: delay=3s finish=killed"
        ));
        assertThat(playersManager.play(taskHolder)).isTrue();

        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_STAGING);

        testScheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_STARTING);

        testScheduler.advanceTimeBy(3, TimeUnit.SECONDS);
        assertThat(taskHolder.getState()).isEqualTo(Protos.TaskState.TASK_KILLED);
    }

    private void initTaskHolder(Map<String, String> env) {
        this.taskHolder = new TaskExecutorHolder(
                playersManager,
                "myJobId",
                "myTaskId",
                agent,
                AwsInstanceType.M4_4XLarge,
                16,
                0,
                65536,
                128_000,
                Collections.emptySet(),
                "myContainerIp",
                "myContainerIp",
                2_000,
                Collections.emptyList(),
                env,
                statusObserver
        );
    }
}