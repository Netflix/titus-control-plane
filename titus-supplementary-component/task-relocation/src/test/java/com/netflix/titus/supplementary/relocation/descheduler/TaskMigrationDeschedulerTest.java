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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.RelocationAttributes;
import com.netflix.titus.runtime.connector.eviction.EvictionConfiguration;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.connector.Node;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.supplementary.relocation.model.DeschedulingFailure;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.testkit.model.job.JobGenerator;
import com.netflix.titus.testkit.model.job.JobTestFunctions;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofServiceSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withApplicationName;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withDisruptionBudget;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withJobId;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.selfManagedPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobTestFunctions.toTaskMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskMigrationDeschedulerTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestClock clock = (TestClock) titusRuntime.getClock();

    private final MutableDataGenerator<Job<ServiceJobExt>> jobGenerator = new MutableDataGenerator<>(
            JobGenerator.serviceJobs(oneTaskServiceJobDescriptor().but(
                    ofServiceSize(4),
                    withDisruptionBudget(budget(selfManagedPolicy(30_000), unlimitedRate(), Collections.emptyList()))
            ))
    );

    private final RelocationConnectorStubs relocationConnectorStubs = new RelocationConnectorStubs()
            .addActiveInstanceGroup("active1", 10)
            .addRemovableInstanceGroup("removable1", 10)
            .addJob(jobGenerator.getValue().but(withJobId("job1")).but(withApplicationName("app1")))
            .addJob(jobGenerator.getValue().but(withJobId("job2")))
            .addJob(jobGenerator.getValue().but(withJobId("jobToMigrate")));

    private final ReadOnlyJobOperations jobOperations = relocationConnectorStubs.getJobOperations();

    private final NodeDataResolver nodeDataResolver = relocationConnectorStubs.getNodeDataResolver();

    @Before
    public void setUp() {
        // So it does not start at 0.
        clock.advanceTime(Duration.ofDays(1));
    }

    @Test
    public void testImmediateMigrations() {
        relocationConnectorStubs.addJobAttribute("jobToMigrate", RelocationAttributes.RELOCATION_REQUIRED_BY_IMMEDIATELY, "" + (clock.wallTime() + 1));

        Task task0 = jobOperations.getTasks("jobToMigrate").get(0);
        relocationConnectorStubs.place("active1", task0);

        Map<String, DeschedulingResult> immediateEvictions = newDescheduler(Collections.emptyMap()).findAllImmediateEvictions();
        assertThat(immediateEvictions).hasSize(1).containsKey(task0.getId());
    }

    @Test
    public void testSelfMigration() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);

        relocationConnectorStubs.place("removable1", job1Task0);
        relocationConnectorStubs.setQuota("job1", 1);

        TaskRelocationPlan job1Task0Plan = TaskRelocationPlan.newBuilder()
                .withTaskId(job1Task0.getId())
                .withRelocationTime(Long.MAX_VALUE / 2)
                .build();

        Optional<Pair<Node, List<Task>>> results = newDescheduler(Collections.singletonMap(job1Task0.getId(), job1Task0Plan)).nextBestMatch();
        assertThat(results).isEmpty();
    }

    @Test
    public void testSelfMigrationAfterDeadline() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);

        relocationConnectorStubs.place("removable1", job1Task0);
        relocationConnectorStubs.setQuota("job1", 1);

        TaskRelocationPlan job1Task0Plan = TaskRelocationPlan.newBuilder()
                .withTaskId(job1Task0.getId())
                .withRelocationTime(clock.wallTime() - 1)
                .build();

        Optional<Pair<Node, List<Task>>> results = newDescheduler(Collections.singletonMap(job1Task0.getId(), job1Task0Plan)).nextBestMatch();
        assertThat(results).isNotEmpty();
    }

    @Test
    public void testFitness() {
        List<Node> removableAgents = nodeDataResolver.resolve().values().stream()
                .filter(n -> n.getServerGroupId().equals("removable1"))
                .collect(Collectors.toList());
        String agent1 = removableAgents.get(0).getId();
        String agent2 = removableAgents.get(1).getId();
        List<Task> tasksOfJob1 = jobOperations.getTasks("job1");
        relocationConnectorStubs.placeOnAgent(agent1, tasksOfJob1.get(0), tasksOfJob1.get(1));
        relocationConnectorStubs.placeOnAgent(agent2, tasksOfJob1.get(2));
        relocationConnectorStubs.setQuota("job1", 1);

        Optional<Pair<Node, List<Task>>> results = newDescheduler(Collections.emptyMap()).nextBestMatch();
        assertThat(results).isPresent();
        assertThat(results.get().getLeft().getId()).isEqualTo(agent2);

        EvictionQuotaTracker evictionQuotaTracker = mock(EvictionQuotaTracker.class);
        when(evictionQuotaTracker.getSystemEvictionQuota()).thenReturn(0L);
        when(evictionQuotaTracker.isSystemDisruptionWindowOpen()).thenReturn(true);
        when(evictionQuotaTracker.getJobEvictionQuota("job1")).thenReturn(1L);

        TaskMigrationDescheduler taskMigrationDescheduler = newDescheduler(evictionQuotaTracker, () -> "app1*");
        Optional<Pair<Node, List<Task>>> results2 = taskMigrationDescheduler.nextBestMatch();
        assertThat(results2).isNotPresent();

        when(evictionQuotaTracker.isSystemDisruptionWindowOpen()).thenReturn(false);
        Optional<Pair<Node, List<Task>>> results3 = taskMigrationDescheduler.nextBestMatch();
        assertThat(results3).isPresent();
        assertThat(results3.get().getLeft().getId()).isEqualTo(agent2);

        // job1 (app1) not exempt from system disruption window
        Optional<Pair<Node, List<Task>>> results4 = newDescheduler(evictionQuotaTracker, () -> "foo*").nextBestMatch();
        assertThat(results4).isNotPresent();
    }

    @Test
    public void testFailures() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);

        relocationConnectorStubs.place("removable1", job1Task0);
        relocationConnectorStubs.setQuota("job1", 0);

        DeschedulingFailure failure = newDescheduler(Collections.emptyMap()).getDeschedulingFailure(job1Task0);
        assertThat(failure.getReasonMessage()).contains("job quota");
    }

    @Test
    public void testJobRequiredMigrationBy() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);
        relocationConnectorStubs.place("active1", job1Task0);
        relocationConnectorStubs.setQuota("job1", 1);
        relocationConnectorStubs.addJobAttribute("job1", RelocationAttributes.RELOCATION_REQUIRED_BY, "" + clock.wallTime());

        clock.advanceTime(Duration.ofSeconds(1));
        Map<String, DeschedulingResult> results = newDescheduler(Collections.emptyMap()).findRequestedJobOrTaskMigrations();
        assertThat(results).isNotEmpty();
    }

    @Test
    public void testTaskRequiredMigration() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);
        relocationConnectorStubs.place("active1", job1Task0);
        relocationConnectorStubs.setQuota("job1", 1);
        relocationConnectorStubs.addTaskAttribute(job1Task0.getId(), RelocationAttributes.RELOCATION_REQUIRED, "true");

        clock.advanceTime(Duration.ofSeconds(1));
        Map<String, DeschedulingResult> results = newDescheduler(Collections.emptyMap()).findRequestedJobOrTaskMigrations();
        assertThat(results).isNotEmpty();
    }

    @Test
    public void testSystemQuotaExemption() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);
        relocationConnectorStubs.place("active1", job1Task0);
        relocationConnectorStubs.addTaskAttribute(job1Task0.getId(), RelocationAttributes.RELOCATION_REQUIRED, "true");

        EvictionQuotaTracker evictionQuotaTracker = mock(EvictionQuotaTracker.class);
        when(evictionQuotaTracker.getSystemEvictionQuota()).thenReturn(0L);
        when(evictionQuotaTracker.isSystemDisruptionWindowOpen()).thenReturn(false);
        when(evictionQuotaTracker.getJobEvictionQuota("job1")).thenReturn(1L);

        Map<String, DeschedulingResult> results = newDescheduler(evictionQuotaTracker, () -> "app2").findRequestedJobOrTaskMigrations();
        assertThat(results).isEmpty();

        Map<String, DeschedulingResult> results2 = newDescheduler(evictionQuotaTracker, () -> "app1").findRequestedJobOrTaskMigrations();
        assertThat(results2).isNotEmpty();

        // system window open with quota = 0
        when(evictionQuotaTracker.isSystemDisruptionWindowOpen()).thenReturn(true);
        Map<String, DeschedulingResult> results3 = newDescheduler(evictionQuotaTracker, () -> "app1").findRequestedJobOrTaskMigrations();
        assertThat(results3).isEmpty();
    }

    @Test
    public void testAgentInstanceRequiredMigration() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);
        relocationConnectorStubs.place("active1", job1Task0);
        relocationConnectorStubs.setQuota("job1", 1);
        job1Task0 = jobOperations.findTaskById(job1Task0.getId()).get().getRight();

        relocationConnectorStubs.markNodeRelocationRequired(job1Task0.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID));
        Optional<Pair<Node, List<Task>>> results = newDescheduler(Collections.emptyMap()).nextBestMatch();
        assertThat(results).isNotEmpty();
    }

    private TaskMigrationDescheduler newDescheduler(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans) {
        Map<String, Task> tasksById = toTaskMap(jobOperations.getTasks());
        //noinspection unchecked
        return new TaskMigrationDescheduler(
                plannedAheadTaskRelocationPlans,
                new EvacuatedAgentsAllocationTracker(nodeDataResolver.resolve(), tasksById),
                new EvictionQuotaTracker(relocationConnectorStubs.getEvictionOperations(), JobTestFunctions.toJobMap(jobOperations.getJobs())),
                () -> "foo|bar",
                jobOperations.getJobs().stream().collect(Collectors.toMap(Job::getId, j -> j)),
                tasksById,
                titusRuntime);
    }

    private TaskMigrationDescheduler newDescheduler(EvictionQuotaTracker evictionQuotaTracker, EvictionConfiguration evictionConfiguration) {
        Map<String, Task> tasksById = toTaskMap(jobOperations.getTasks());
        //noinspection unchecked
        return new TaskMigrationDescheduler(
                Collections.emptyMap(),
                new EvacuatedAgentsAllocationTracker(nodeDataResolver.resolve(), tasksById),
                evictionQuotaTracker,
                evictionConfiguration,
                jobOperations.getJobs().stream().collect(Collectors.toMap(Job::getId, j -> j)),
                tasksById,
                titusRuntime);
    }
}