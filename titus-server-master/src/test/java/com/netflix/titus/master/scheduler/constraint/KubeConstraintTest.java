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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.client.KubeApiFacade;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Taint;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.scheduler.SchedulerTestUtils.INSTANCE_GROUP_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.INSTANCE_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.TASK_ID;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createAgentInstance;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createTaskRequest;
import static com.netflix.titus.master.scheduler.SchedulerTestUtils.createVirtualMachineCurrentStateMock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KubeConstraintTest {

    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
    private final MesosConfiguration mesosConfiguration = mock(MesosConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final KubeApiFacade kubeApiFacade = createKubeApiFacade();
    private final Indexer<V1Node> indexer = kubeApiFacade.getNodeInformer().getIndexer();
    private final TaskTrackerState taskTrackerState = mock(TaskTrackerState.class);

    private final KubeConstraint kubeConstraint = new KubeConstraint(schedulerConfiguration, mesosConfiguration, agentManagementService, kubeApiFacade);

    @Before
    public void setUp() throws Exception {
        when(schedulerConfiguration.getInstanceAttributeName()).thenReturn("id");
        when(mesosConfiguration.isKubeApiServerIntegrationEnabled()).thenReturn(true);
        when(mesosConfiguration.getFenzoTaintTolerations()).thenReturn(Collections.singleton("tolerated_taint_key"));
    }

    @Test
    public void instanceNotFound() {
        when(agentManagementService.findAgentInstance(any())).thenReturn(Optional.empty());
        ConstraintEvaluator.Result result = kubeConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase(KubeConstraint.INSTANCE_NOT_FOUND_REASON);
    }

    @Test
    public void nodeNotFound() {
        AgentInstance agentInstance = createAgentInstance(INSTANCE_ID, INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(agentInstance));

        when(indexer.getByKey(INSTANCE_ID)).thenReturn(null);

        ConstraintEvaluator.Result result = kubeConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase(KubeConstraint.NODE_NOT_FOUND_REASON);
    }

    @Test
    public void nodeNotReady() {
        AgentInstance agentInstance = createAgentInstance(INSTANCE_ID, INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(agentInstance));

        V1Node node = createNode(INSTANCE_ID, false, Collections.emptyList());
        when(indexer.getByKey(INSTANCE_ID)).thenReturn(node);

        ConstraintEvaluator.Result result = kubeConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase(KubeConstraint.NODE_NOT_READY_REASON);
    }

    @Test
    public void taintNotToleratedInConfiguration() {
        AgentInstance agentInstance = createAgentInstance(INSTANCE_ID, INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(agentInstance));

        V1Taint taint = createTaint("not_tolerated_taint_key", "NoSchedule", "not_tolerated_taint_value");
        V1Node node = createNode(INSTANCE_ID, true, Collections.singletonList(taint));
        when(indexer.getByKey(INSTANCE_ID)).thenReturn(node);

        ConstraintEvaluator.Result result = kubeConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isFalse();
        assertThat(result.getFailureReason()).isEqualToIgnoringCase(KubeConstraint.TAINT_NOT_TOLERATED_IN_CONFIGURATION_REASON);
    }

    @Test
    public void successfullyScheduled() {
        AgentInstance agentInstance = createAgentInstance(INSTANCE_ID, INSTANCE_GROUP_ID);
        when(agentManagementService.findAgentInstance(INSTANCE_ID)).thenReturn(Optional.of(agentInstance));

        V1Taint taint = createTaint("tolerated_taint_key", "NoSchedule", "tolerated_taint_value");
        V1Node node = createNode(INSTANCE_ID, true, Collections.singletonList(taint));
        when(indexer.getByKey(INSTANCE_ID)).thenReturn(node);

        ConstraintEvaluator.Result result = kubeConstraint.evaluate(
                createTaskRequest(TASK_ID),
                createVirtualMachineCurrentStateMock(INSTANCE_ID, Collections.emptyList(), Collections.emptyList()),
                taskTrackerState);
        assertThat(result.isSuccessful()).isTrue();
    }

    private KubeApiFacade createKubeApiFacade() {
        Indexer<V1Node> indexer = mock(Indexer.class);
        SharedIndexInformer<V1Node> nodeInformer = mock(SharedIndexInformer.class);
        KubeApiFacade kubeApiFacade = mock(KubeApiFacade.class);

        when(kubeApiFacade.getNodeInformer()).thenReturn(nodeInformer);
        when(nodeInformer.getIndexer()).thenReturn(indexer);

        return kubeApiFacade;
    }

    private V1Node createNode(String name, Boolean ready, List<V1Taint> taints) {
        return new V1Node()
                .metadata(
                        new V1ObjectMeta().name(name)
                )
                .status(
                        new V1NodeStatus()
                                .addConditionsItem(new V1NodeCondition().type(KubeConstraint.READY).status(ready.toString()))
                ).spec(
                        new V1NodeSpec().taints(taints)
                );
    }

    private V1Taint createTaint(String key, String effect, String value) {
        return new V1Taint().key(key).effect(effect).value(value);
    }
}