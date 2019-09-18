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

package com.netflix.titus.master.clusteroperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.TaskPlacementFailure;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.master.scheduler.TaskPlacementFailure.FailureKind.AllAgentsFull;
import static com.netflix.titus.master.scheduler.TaskPlacementFailure.FailureKind.OpportunisticResource;
import static com.netflix.titus.master.scheduler.TaskPlacementFailure.FailureKind.WaitingForInUseIpAllocation;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterAgentAutoScalerTest {

    private final TestScheduler testScheduler = Schedulers.test();
    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);
    private final ClusterOperationsConfiguration configuration = mock(ClusterOperationsConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
    @SuppressWarnings("unchecked")
    private final SchedulingService<? extends TaskRequest> schedulingService = mock(SchedulingService.class);

    @Before
    public void setUp() throws Exception {
        when(configuration.isAutoScalingAgentsEnabled()).thenReturn(true);

        when(configuration.getCriticalPrimaryInstanceType()).thenReturn("m4.16xlarge");
        when(configuration.getCriticalMinIdle()).thenReturn(5);
        when(configuration.getCriticalMaxIdle()).thenReturn(10);
        when(configuration.getCriticalScaleUpCoolDownMs()).thenReturn(600000L);
        when(configuration.getCriticalScaleDownCoolDownMs()).thenReturn(600000L);
        when(configuration.getCriticalTaskSloMs()).thenReturn(90000L);
        when(configuration.getCriticalIdleInstanceGracePeriodMs()).thenReturn(90000L);

        when(configuration.getFlexPrimaryInstanceType()).thenReturn("r4.16xlarge");
        when(configuration.getFlexMinIdle()).thenReturn(5);
        when(configuration.getFlexMaxIdle()).thenReturn(10);
        when(configuration.getFlexScaleUpCoolDownMs()).thenReturn(60000L);
        when(configuration.getFlexScaleDownCoolDownMs()).thenReturn(60000L);
        when(configuration.getFlexTaskSloMs()).thenReturn(300000L);
        when(configuration.getFlexIdleInstanceGracePeriodMs()).thenReturn(90000L);

        when(configuration.getAgentInstanceRemovableTimeoutMs()).thenReturn(600000L);

        ResourceDimension resourceDimension = ResourceDimension.newBuilder()
                .withCpus(64)
                .withMemoryMB(99999)
                .withDiskMB(9999)
                .withNetworkMbs(23000)
                .build();
        when(agentManagementService.getResourceLimits(any())).thenReturn(resourceDimension);
        when(agentManagementService.updateAgentInstanceAttributes(any(), any())).thenReturn(Completable.complete());
    }

    @Test
    public void testScaleUpMinIdle() {
        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService).scaleUp("instanceGroup1", 5);
    }

    @Test
    public void testScaleDownMaxIdle() {
        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(12)
                .withMax(20)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = createAgents(12, "instanceGroup1", false);
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(2)).updateAgentInstanceAttributes(any(), any());
    }

    @Test
    public void testScaleUpForFailingTasks() {
        when(configuration.getFlexMinIdle()).thenReturn(0);

        Job job = createJob();
        when(v3JobOperations.getJobs()).thenReturn(Collections.singletonList(job));

        List<Task> tasks = createTasks(10, "jobId");
        when(v3JobOperations.getTasks()).thenReturn(tasks);

        Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> taskPlacementFailures = createTaskPlacementFailures(ImmutableMap.of(
                AllAgentsFull, 10
        ), Tier.Flex);
        doReturn(taskPlacementFailures).when(schedulingService).getLastTaskPlacementFailures();

        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(20)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService).scaleUp("instanceGroup1", 10);
    }

    @Test
    public void testNoScaleUpForSomeFailureKinds() {
        when(configuration.getFlexMinIdle()).thenReturn(0);

        Job job = createJob();
        when(v3JobOperations.getJobs()).thenReturn(Collections.singletonList(job));

        List<Task> tasks = createTasks(10, "jobId");
        when(v3JobOperations.getTasks()).thenReturn(tasks);

        Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> taskPlacementFailures = createTaskPlacementFailures(ImmutableMap.of(
                WaitingForInUseIpAllocation, 2,
                OpportunisticResource, 8
        ), Tier.Flex);
        doReturn(taskPlacementFailures).when(schedulingService).getLastTaskPlacementFailures();

        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(20)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();
        verify(agentManagementService, never()).scaleUp(anyString(), anyInt());
    }

    @Test
    public void testScaleUpForTasksPastSlo() {
        when(configuration.getFlexMinIdle()).thenReturn(0);
        when(configuration.getFlexScaleUpCoolDownMs()).thenReturn(72000000L);
        when(configuration.getFlexTaskSloMs()).thenReturn(3600000L);

        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        Job job = createJob();
        when(v3JobOperations.getJobs()).thenReturn(Collections.singletonList(job));

        List<Task> tasks = createTasks(10, "jobId");
        when(v3JobOperations.getTasks()).thenReturn(tasks);

        Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> taskPlacementFailures = createTaskPlacementFailures(ImmutableMap.of(
                AllAgentsFull, 10
        ), Tier.Flex);
        doReturn(taskPlacementFailures).when(schedulingService).getLastTaskPlacementFailures();

        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(1, TimeUnit.HOURS);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService).scaleUp("instanceGroup1", 10);
    }

    @Test
    public void testNoScaleUpForTasksPastSloWithSomeFailureKinds() {
        when(configuration.getFlexMinIdle()).thenReturn(0);
        when(configuration.getFlexScaleUpCoolDownMs()).thenReturn(72000000L);
        when(configuration.getFlexTaskSloMs()).thenReturn(3600000L);

        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        Job job = createJob();
        when(v3JobOperations.getJobs()).thenReturn(Collections.singletonList(job));

        List<Task> tasks = createTasks(10, "jobId");
        when(v3JobOperations.getTasks()).thenReturn(tasks);

        Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> taskPlacementFailures = createTaskPlacementFailures(ImmutableMap.of(
                WaitingForInUseIpAllocation, 2,
                OpportunisticResource, 8
        ), Tier.Flex);
        doReturn(taskPlacementFailures).when(schedulingService).getLastTaskPlacementFailures();

        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(1, TimeUnit.HOURS);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, never()).scaleUp(anyString(), anyInt());
    }

    @Test
    public void testScaleUpForNonPrimaryInstanceType() {
        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("m4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(0)).scaleUp(anyString(), anyInt());
    }

    @Test
    public void testScaleUpForActiveAndPhasedOutInstanceGroups() {
        AgentInstanceGroup activeInstanceGroup = AgentInstanceGroup.newBuilder()
                .withId("activeInstanceGroup")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        AgentInstanceGroup phasedOutInstanceGroup = AgentInstanceGroup.newBuilder()
                .withId("phasedOutInstanceGroup")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.PhasedOut)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(0)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        List<AgentInstanceGroup> instanceGroups = Lists.newArrayList(phasedOutInstanceGroup, activeInstanceGroup);
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        when(agentManagementService.getAgentInstances("activeInstanceGroup")).thenReturn(Collections.emptyList());
        when(agentManagementService.getAgentInstances("phasedOutInstanceGroup")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("activeInstanceGroup"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(1)).scaleUp(eq("activeInstanceGroup"), eq(5));
        verify(agentManagementService, times(0)).scaleUp(eq("phasedOutInstanceGroup"), anyInt());
    }

    @Test
    public void testScaleDownForNonPrimaryInstanceType() {
        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("m4.16xlarge")
                .withMin(0)
                .withCurrent(12)
                .withMax(20)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = createAgents(12, "instanceGroup1", false);
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(0)).updateAgentInstanceAttributes(any(), any());
    }

    @Test
    public void testScaleDownForIdleAgents() {
        when(configuration.getFlexMinIdle()).thenReturn(0);
        when(configuration.getFlexMaxIdle()).thenReturn(0);

        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(20)
                .withMax(20)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = createAgents(20, "instanceGroup1", false);
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);

        List<Task> tasks = createTasks(17, "jobId");
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            when(task.getTaskContext()).thenReturn(singletonMap(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, "instanceGroup1" + i));
        }

        when(v3JobOperations.getTasks()).thenReturn(tasks);

        testScheduler.advanceTimeBy(11, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(3)).updateAgentInstanceAttributes(any(), any());
    }

    @Test
    public void testScaleDownForIdleAgentsPrefersPhasedOutInstanceGroups() {
        when(configuration.getFlexMinIdle()).thenReturn(0);
        when(configuration.getFlexMaxIdle()).thenReturn(5);

        AgentInstanceGroup activeInstanceGroup = AgentInstanceGroup.newBuilder()
                .withId("activeInstanceGroup")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(5)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        AgentInstanceGroup phasedOutInstanceGroup = AgentInstanceGroup.newBuilder()
                .withId("phasedOutInstanceGroup")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.PhasedOut)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(0)
                .withCurrent(5)
                .withMax(10)
                .withAttributes(Collections.emptyMap())
                .build();
        List<AgentInstanceGroup> instanceGroups = Lists.newArrayList(phasedOutInstanceGroup, activeInstanceGroup);
        when(agentManagementService.getInstanceGroups()).thenReturn(instanceGroups);

        List<AgentInstance> activeInstances = createAgents(5, "activeInstanceGroup", false);
        when(agentManagementService.getAgentInstances("activeInstanceGroup")).thenReturn(activeInstances);

        List<AgentInstance> phasedOutInstances = createAgents(5, "phasedOutInstanceGroup", false);
        when(agentManagementService.getAgentInstances("phasedOutInstanceGroup")).thenReturn(phasedOutInstances);

        when(v3JobOperations.getTasks()).thenReturn(Collections.emptyList());

        testScheduler.advanceTimeBy(11, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(5)).updateAgentInstanceAttributes(matches("phasedOutInstanceGroup.*"), any());
        verify(agentManagementService, times(0)).updateAgentInstanceAttributes(matches("activeInstanceGroup.*"), any());
    }

    @Test
    public void testScaleDownForIdleAgentsDoesNotGoPastInstanceGroupMin() {
        when(configuration.getFlexMinIdle()).thenReturn(0);
        when(configuration.getAgentInstanceRemovableTimeoutMs()).thenReturn(9999999999L);

        AgentInstanceGroup instanceGroup = AgentInstanceGroup.newBuilder()
                .withId("instanceGroup1")
                .withTier(Tier.Flex)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder()
                        .withState(InstanceGroupLifecycleState.Active)
                        .withTimestamp(titusRuntime.getClock().wallTime())
                        .build())
                .withInstanceType("r4.16xlarge")
                .withMin(13)
                .withCurrent(20)
                .withMax(20)
                .withAttributes(Collections.emptyMap())
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = CollectionsExt.merge(
                createAgents(18, "instanceGroup1", false),
                createAgents(2, "instanceGroup1", true)
        );
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);

        when(v3JobOperations.getTasks()).thenReturn(emptyList());

        testScheduler.advanceTimeBy(11, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(5)).updateAgentInstanceAttributes(any(), any());
    }

    private Map<String, String> createAgentAttributesWithRemovable(long timestamp) {
        return Collections.singletonMap(ClusterOperationsAttributes.REMOVABLE, String.valueOf(timestamp));
    }

    private List<AgentInstance> createAgents(int count, String instanceGroupId, boolean removable) {
        List<AgentInstance> agents = new ArrayList<>();
        long now = titusRuntime.getClock().wallTime();
        for (int i = 0; i < count; i++) {
            Map<String, String> attributes = removable ? createAgentAttributesWithRemovable(now) : Collections.emptyMap();
            AgentInstance agentInstance = AgentInstance.newBuilder()
                    .withId(instanceGroupId + i)
                    .withInstanceGroupId(instanceGroupId)
                    .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Started).withLaunchTimestamp(now).build())
                    .withAttributes(attributes)
                    .build();
            agents.add(agentInstance);
        }
        return agents;
    }

    private Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> createTaskPlacementFailures(Map<TaskPlacementFailure.FailureKind, Integer> count,
                                                                                                                       Tier tier) {
        Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> failureKinds = new HashMap<>();
        for (Map.Entry<TaskPlacementFailure.FailureKind, Integer> entry : count.entrySet()) {
            TaskPlacementFailure.FailureKind failureKind = entry.getKey();
            for (int i = 0; i < entry.getValue(); i++) {
                Map<String, List<TaskPlacementFailure>> failuresByTaskId = failureKinds.computeIfAbsent(failureKind, k -> new HashMap<>());
                String taskId = "task" + i;
                failuresByTaskId.computeIfAbsent(taskId, k -> new ArrayList<>()).add(new TaskPlacementFailure(taskId, failureKind, -1, tier, Collections.emptyMap()));
            }
        }
        return failureKinds;
    }

    private Job createJob() {
        Job job = mock(Job.class);
        ContainerResources containerResources = ContainerResources.newBuilder()
                .withCpu(64)
                .build();
        when(job.getId()).thenReturn("jobId");

        Container container = mock(Container.class);
        when(container.getContainerResources()).thenReturn(containerResources);
        JobDescriptor jobDescriptor = mock(JobDescriptor.class);
        when(jobDescriptor.getContainer()).thenReturn(container);
        when(job.getJobDescriptor()).thenReturn(jobDescriptor);
        return job;
    }

    private List<Task> createTasks(int count, String jobId) {
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Task task = mock(Task.class);
            when(task.getId()).thenReturn("task" + i);
            when(task.getJobId()).thenReturn(jobId);
            TaskStatus taskStatus = TaskStatus.newBuilder()
                    .withState(TaskState.Accepted)
                    .withTimestamp(titusRuntime.getClock().wallTime())
                    .build();
            when(task.getStatus()).thenReturn(taskStatus);
            tasks.add(task);
        }
        return tasks;
    }
}