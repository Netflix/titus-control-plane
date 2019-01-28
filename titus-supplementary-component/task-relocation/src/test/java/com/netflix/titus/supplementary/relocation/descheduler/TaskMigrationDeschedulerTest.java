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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.ReadOnlyAgentOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.RelocationConnectorStubs;
import com.netflix.titus.supplementary.relocation.model.DeschedulingFailure;
import com.netflix.titus.testkit.model.job.JobGenerator;
import com.netflix.titus.testkit.model.job.JobTestFunctions;
import org.junit.Test;

import static com.netflix.titus.api.agent.model.AgentFunctions.withId;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofServiceSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withDisruptionBudget;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withJobDisruptionBudget;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withJobId;
import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentServerGroups;
import static com.netflix.titus.testkit.model.agent.AgentTestFunctions.inState;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.selfManagedPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobTestFunctions.toTaskMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TaskMigrationDeschedulerTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final MutableDataGenerator<AgentInstanceGroup> flexInstanceGroupGenerator = new MutableDataGenerator<>(agentServerGroups(Tier.Flex, 10));

    private final MutableDataGenerator<Job<ServiceJobExt>> jobGenerator = new MutableDataGenerator<>(
            JobGenerator.serviceJobs(oneTaskServiceJobDescriptor().but(
                    ofServiceSize(4),
                    withDisruptionBudget(budget(selfManagedPolicy(30_000), unlimitedRate(), Collections.emptyList()))
            ))
    );

    private final RelocationConnectorStubs dataGenerator = new RelocationConnectorStubs()
            .addInstanceGroup(flexInstanceGroupGenerator.getValue().but(withId("active1"), inState(InstanceGroupLifecycleState.Active)))
            .addInstanceGroup(flexInstanceGroupGenerator.getValue().but(withId("removable1"), inState(InstanceGroupLifecycleState.Removable)))
            .addJob(jobGenerator.getValue().but(withJobId("job1")))
            .addJob(jobGenerator.getValue().but(withJobId("job2")));

    private final ReadOnlyJobOperations jobOperations = dataGenerator.getJobOperations();
    private final ReadOnlyAgentOperations agentOperations = dataGenerator.getAgentOperations();

    @Test
    public void testDelayedMigrations() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);

        dataGenerator.place("removable1", job1Task0);
        dataGenerator.setQuota("job1", 1);

        TaskRelocationPlan job1Task0Plan = TaskRelocationPlan.newBuilder()
                .withTaskId(job1Task0.getId())
                .withRelocationTime(Long.MAX_VALUE / 2)
                .build();

        Optional<Pair<AgentInstance, List<Task>>> results = newDescheduler(Collections.singletonMap(job1Task0.getId(), job1Task0Plan)).nextBestMatch();
        assertThat(results).isEmpty();
    }

    @Test
    public void testFitness() {
        List<AgentInstance> removableAgents = agentOperations.getAgentInstances("removable1");
        String agent1 = removableAgents.get(0).getId();
        String agent2 = removableAgents.get(1).getId();
        List<Task> tasksOfJob1 = jobOperations.getTasks("job1");
        dataGenerator.placeOnAgent(agent1, tasksOfJob1.get(0), tasksOfJob1.get(1));
        dataGenerator.placeOnAgent(agent2, tasksOfJob1.get(2));
        dataGenerator.setQuota("job1", 1);

        Optional<Pair<AgentInstance, List<Task>>> results = newDescheduler(Collections.emptyMap()).nextBestMatch();
        assertThat(results).isPresent();
        assertThat(results.get().getLeft().getId()).isEqualTo(agent2);
    }

    @Test
    public void testFailures() {
        Task job1Task0 = jobOperations.getTasks("job1").get(0);

        dataGenerator.place("removable1", job1Task0);
        dataGenerator.setQuota("job1", 0);

        DeschedulingFailure failure = newDescheduler(Collections.emptyMap()).getDeschedulingFailure(job1Task0);
        assertThat(failure.getReasonMessage()).contains("job quota");
    }

    @Test
    public void testLegacyJobs() {
        dataGenerator.addJob(jobGenerator.getValue().but(withJobId("jobLegacy"), withJobDisruptionBudget(DisruptionBudget.none())));
        Task task0 = jobOperations.getTasks("jobLegacy").get(0);
        dataGenerator.place("removable1", task0);
        dataGenerator.setQuota("jobLegacy", 1);

        Optional<Pair<AgentInstance, List<Task>>> results = newDescheduler(Collections.emptyMap()).nextBestMatch();
        assertThat(results).isEmpty();
    }

    private TaskMigrationDescheduler newDescheduler(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans) {
        Map<String, Task> tasksById = toTaskMap(jobOperations.getTasks());
        return new TaskMigrationDescheduler(
                plannedAheadTaskRelocationPlans,
                new EvacuatedAgentsAllocationTracker(dataGenerator.getAgentOperations(), tasksById),
                new EvictionQuotaTracker(dataGenerator.getEvictionOperations(), JobTestFunctions.toJobMap(jobOperations.getJobs())),
                jobOperations.getJobs().stream().collect(Collectors.toMap(Job::getId, j -> j)),
                tasksById,
                titusRuntime
        );
    }
}