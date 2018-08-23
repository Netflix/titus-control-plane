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

package com.netflix.titus.master.clusteroperations;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.api.agent.model.InstanceOverrideStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Either;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterRemovableAgentRemoverTest {

    private final TestScheduler testScheduler = Schedulers.test();
    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);
    private final ClusterOperationsConfiguration configuration = mock(ClusterOperationsConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);

    @Before
    public void setUp() throws Exception {
        when(configuration.isRemovingAgentsEnabled()).thenReturn(true);
    }

    @Test
    public void testClusterAgentRemoval() {
        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        AgentInstance agentInstance1 = AgentInstance.newBuilder()
                .withId("agentInstance1")
                .withInstanceGroupId("instanceGroup1")
                .withOverrideStatus(InstanceOverrideStatus.newBuilder().withState(InstanceOverrideState.Removable).build())
                .build();
        AgentInstance agentInstance2 = AgentInstance.newBuilder()
                .withId("agentInstance2")
                .withInstanceGroupId("instanceGroup1")
                .withOverrideStatus(InstanceOverrideStatus.newBuilder().withState(InstanceOverrideState.None).build())
                .build();

        List<AgentInstance> agentInstances = asList(agentInstance1, agentInstance2);
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);

        when(agentManagementService.terminateAgents("instanceGroup1", singletonList("agentInstance1"), true))
                .thenReturn(Observable.just(singletonList(Either.ofValue(true))));

        Task task = createTask("agentInstance2");
        when(v3JobOperations.getTasks()).thenReturn(singletonList(task));

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterRemovableAgentRemover clusterRemovableAgentRemover = new ClusterRemovableAgentRemover(titusRuntime, configuration,
                agentManagementService, v3JobOperations, testScheduler);

        clusterRemovableAgentRemover.doRemoveAgents().await();

        verify(agentManagementService).terminateAgents("instanceGroup1", singletonList("agentInstance1"), true);
    }

    private Task createTask(String agentId) {
        Task task = mock(Task.class);
        when(task.getTaskContext()).thenReturn(singletonMap(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, agentId));
        return task;
    }
}