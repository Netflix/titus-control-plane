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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.api.agent.model.InstanceOverrideStatus;
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
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.TaskPlacementFailure;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterAgentAutoScalerTest {

    private final TestScheduler testScheduler = Schedulers.test();
    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);
    private final ClusterOperationsConfiguration configuration = mock(ClusterOperationsConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);
    private final SchedulingService schedulingService = mock(SchedulingService.class);

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
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = createAgents(12, "instanceGroup1");
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);
        when(agentManagementService.updateInstanceOverride(any(), any())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(6, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(2)).updateInstanceOverride(any(), any());
    }

    @Test
    public void testScaleUpForFailingTasks() {
        when(configuration.getFlexMinIdle()).thenReturn(0);

        Job job = createJob();
        when(v3JobOperations.getJobs()).thenReturn(Collections.singletonList(job));

        List<Task> tasks = createTasks(10, "jobId");
        when(v3JobOperations.getTasks()).thenReturn(tasks);

        Map<TaskPlacementFailure.FailureKind, List<TaskPlacementFailure>> taskPlacementFailures = createTaskPlacementFailures(ImmutableMap.of(
                TaskPlacementFailure.FailureKind.AllAgentsFull, 10
        ), Tier.Flex);
        when(schedulingService.getLastTaskPlacementFailures()).thenReturn(taskPlacementFailures);

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

        Map<TaskPlacementFailure.FailureKind, List<TaskPlacementFailure>> taskPlacementFailures = createTaskPlacementFailures(ImmutableMap.of(
                TaskPlacementFailure.FailureKind.AllAgentsFull, 10
        ), Tier.Flex);
        when(schedulingService.getLastTaskPlacementFailures()).thenReturn(taskPlacementFailures);

        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(Collections.emptyList());
        when(agentManagementService.scaleUp(eq("instanceGroup1"), anyInt())).thenReturn(Completable.complete());

        testScheduler.advanceTimeBy(1, TimeUnit.HOURS);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService).scaleUp("instanceGroup1", 10);
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
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = createAgents(20, "instanceGroup1");
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);
        when(agentManagementService.updateInstanceOverride(any(), any())).thenReturn(Completable.complete());

        List<Task> tasks = createTasks(17, "jobId");
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            when(task.getTaskContext()).thenReturn(singletonMap(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, "agentInstance" + i));
        }

        when(v3JobOperations.getTasks()).thenReturn(tasks);

        testScheduler.advanceTimeBy(11, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(3)).updateInstanceOverride(any(), any());
    }

    @Test
    public void testScaleDownForIdleAgentsDoesNotGoPastInstanceGroupMin() {
        when(configuration.getFlexMinIdle()).thenReturn(0);

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
                .build();
        when(agentManagementService.getInstanceGroups()).thenReturn(singletonList(instanceGroup));

        List<AgentInstance> agentInstances = createAgents(20, "instanceGroup1");
        when(agentManagementService.getAgentInstances("instanceGroup1")).thenReturn(agentInstances);
        when(agentManagementService.updateInstanceOverride(any(), any())).thenReturn(Completable.complete());

        when(v3JobOperations.getTasks()).thenReturn(emptyList());

        testScheduler.advanceTimeBy(11, TimeUnit.MINUTES);

        ClusterAgentAutoScaler clusterAgentAutoScaler = new ClusterAgentAutoScaler(titusRuntime, configuration,
                agentManagementService, v3JobOperations, schedulingService, testScheduler);

        clusterAgentAutoScaler.doAgentScaling().await();

        verify(agentManagementService, times(7)).updateInstanceOverride(any(), any());
    }

    private List<AgentInstance> createAgents(int count, String instanceGroupId) {
        List<AgentInstance> agents = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            AgentInstance agentInstance = AgentInstance.newBuilder()
                    .withId("agentInstance" + i)
                    .withInstanceGroupId(instanceGroupId)
                    .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Started).withLaunchTimestamp(titusRuntime.getClock().wallTime()).build())
                    .withOverrideStatus(InstanceOverrideStatus.newBuilder().withState(InstanceOverrideState.None).build())
                    .build();
            agents.add(agentInstance);
        }
        return agents;
    }

    private Map<TaskPlacementFailure.FailureKind, List<TaskPlacementFailure>> createTaskPlacementFailures(Map<TaskPlacementFailure.FailureKind, Integer> count,
                                                                                                          Tier tier) {
        Map<TaskPlacementFailure.FailureKind, List<TaskPlacementFailure>> failureKinds = new HashMap<>();
        for (Map.Entry<TaskPlacementFailure.FailureKind, Integer> entry : count.entrySet()) {
            TaskPlacementFailure.FailureKind failureKind = entry.getKey();
            List<TaskPlacementFailure> failures = failureKinds.computeIfAbsent(failureKind, k -> new ArrayList<>());
            for (int i = 0; i < entry.getValue(); i++) {
                TaskPlacementFailure failure = new TaskPlacementFailure("task" + i, failureKind, -1, tier, Collections.emptyMap());
                failures.add(failure);
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