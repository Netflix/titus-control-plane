/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.controller;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.time.internal.DefaultTestClock;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import rx.Observable;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LABEL_ACCOUNT_ID;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.READY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeGcControllerTest {
    private static final long NODE_GC_GRACE_PERIOD = 1000L;
    private static final String NODE_NAME = "node-name";
    private static final String CORRECT_ACCOUNT = "correct-account";
    private static final String INCORRECT_ACCOUNT = "incorrect-account";
    private static final TestClock clock = new DefaultTestClock();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);
    private final LocalScheduler scheduler = mock(LocalScheduler.class);
    private final FixedIntervalTokenBucketConfiguration tokenBucketConfiguration = mock(FixedIntervalTokenBucketConfiguration.class);
    private final ControllerConfiguration controllerConfiguration = mock(ControllerConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final KubeApiFacade kubeApiFacade = mock(KubeApiFacade.class);
    private final KubeControllerConfiguration kubeControllerConfiguration = mock(KubeControllerConfiguration.class);

    private final NodeGcController nodeGcController = new NodeGcController(
            titusRuntime,
            scheduler,
            tokenBucketConfiguration,
            controllerConfiguration,
            agentManagementService,
            kubeApiFacade,
            kubeControllerConfiguration
    );


    @BeforeEach
    void setUp() {
        when(kubeControllerConfiguration.getAccountId()).thenReturn(CORRECT_ACCOUNT);
        when(kubeControllerConfiguration.getNodeGcGracePeriodMs()).thenReturn(NODE_GC_GRACE_PERIOD);
    }

    /**
     * The node is in the wrong account and should return false.
     */
    @Test
    void nodeIsInWrongAccount() {
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, INCORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(null);
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isFalse();
    }

    /**
     * The node object is missing information and should return false.
     */
    @Test
    void nodeIsInvalid() {
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(null);
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isFalse();
    }

    /**
     * The node condition ready last heartbeat time should return false if it is not past the configured grace period.
     */
    @Test
    void nodeReadyConditionTimestampIsNotPastGracePeriod() {
        long now = clock.wallTime();
        V1NodeCondition readyCondition = new V1NodeCondition().type(READY).lastHeartbeatTime(DateTimeExt.fromMillis(now));
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(new V1NodeStatus().addConditionsItem(readyCondition));
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isFalse();
    }

    /**
     * The node condition ready last heartbeat time should return true if it is past the configured grace period.
     */
    @Test
    void nodeReadyConditionTimestampIsPastGracePeriod() {
        long now = clock.wallTime();
        clock.advanceTime(Duration.ofMillis(NODE_GC_GRACE_PERIOD + 1));
        V1NodeCondition readyCondition = new V1NodeCondition().type(READY).lastHeartbeatTime(DateTimeExt.fromMillis(now));
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(new V1NodeStatus().addConditionsItem(readyCondition));
        AgentInstance agentInstance = AgentInstance.newBuilder()
                .withId(NODE_NAME)
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Stopped).build())
                .build();
        when(agentManagementService.findAgentInstance(NODE_NAME)).thenReturn(Optional.of(agentInstance));
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isTrue();
    }

    /**
     * The agent instance for the node object does not exist in agent management and should return true.
     */
    @Test
    void agentInstanceNotInAgentManagement() {
        long now = clock.wallTime();
        clock.advanceTime(Duration.ofMillis(NODE_GC_GRACE_PERIOD + 1));
        V1NodeCondition readyCondition = new V1NodeCondition().type(READY).lastHeartbeatTime(DateTimeExt.fromMillis(now));
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(new V1NodeStatus().addConditionsItem(readyCondition));
        when(agentManagementService.findAgentInstance(NODE_NAME)).thenReturn(Optional.empty());
        when(agentManagementService.getAgentInstanceAsync(NODE_NAME)).thenReturn(Mono.empty());
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isTrue();
    }

    /**
     * The agent instance for the node object does not exist in agent management cache and should make a separate
     * call out to AWS in order to fetch the instance status
     */
    @Test
    void agentInstanceNotInAgentManagementCache() {
        long now = clock.wallTime();
        clock.advanceTime(Duration.ofMillis(NODE_GC_GRACE_PERIOD + 1));
        V1NodeCondition readyCondition = new V1NodeCondition().type(READY).lastHeartbeatTime(DateTimeExt.fromMillis(now));
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(new V1NodeStatus().addConditionsItem(readyCondition));
        when(agentManagementService.findAgentInstance(NODE_NAME)).thenReturn(Optional.empty());

        // instance state = stopped
        AgentInstance agentInstance = AgentInstance.newBuilder()
                .withId(NODE_NAME)
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Stopped).build())
                .build();
        when(agentManagementService.getAgentInstanceAsync(NODE_NAME)).thenReturn(Mono.just(agentInstance));
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isTrue();

        // instance state != stopped
        AgentInstance agentInstance2 = AgentInstance.newBuilder()
                .withId(NODE_NAME)
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Launching).build())
                .build();
        when(agentManagementService.getAgentInstanceAsync(NODE_NAME)).thenReturn(Mono.just(agentInstance2));
        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isFalse();
    }

    /**
     * The agent instance is not in the stopped state and should return false.
     */
    @Test
    void agentInstanceIsNotStopped() {
        long now = clock.wallTime();
        clock.advanceTime(Duration.ofMillis(NODE_GC_GRACE_PERIOD + 1));
        V1NodeCondition readyCondition = new V1NodeCondition().type(READY).lastHeartbeatTime(DateTimeExt.fromMillis(now));
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(new V1NodeStatus().addConditionsItem(readyCondition));

        AgentInstance agentInstance = AgentInstance.newBuilder()
                .withId(NODE_NAME)
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Started).build())
                .build();
        when(agentManagementService.findAgentInstance(NODE_NAME)).thenReturn(Optional.of(agentInstance));

        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isFalse();
    }

    /**
     * The agent instance is in the stopped state and should return true.
     */
    @Test
    void agentInstanceIsStopped() {
        long now = clock.wallTime();
        clock.advanceTime(Duration.ofMillis(NODE_GC_GRACE_PERIOD + 1));
        V1NodeCondition readyCondition = new V1NodeCondition().type(READY).lastHeartbeatTime(DateTimeExt.fromMillis(now));
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().name(NODE_NAME).annotations(Collections.singletonMap(NODE_LABEL_ACCOUNT_ID, CORRECT_ACCOUNT)))
                .spec(new V1NodeSpec())
                .status(new V1NodeStatus().addConditionsItem(readyCondition));

        AgentInstance agentInstance = AgentInstance.newBuilder()
                .withId(NODE_NAME)
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Stopped).build())
                .build();
        when(agentManagementService.findAgentInstance(NODE_NAME)).thenReturn(Optional.of(agentInstance));

        Assertions.assertThat(nodeGcController.isNodeEligibleForGc(node)).isTrue();
    }
}