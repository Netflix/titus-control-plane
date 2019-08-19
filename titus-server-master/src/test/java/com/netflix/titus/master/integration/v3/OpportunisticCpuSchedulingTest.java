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

package com.netflix.titus.master.integration.v3;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.twoPartitionsPerTierCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class OpportunisticCpuSchedulingTest extends BaseIntegrationTest {
    private static final JobDescriptor<BatchJobExt> BATCH_JOB_WITH_RUNTIME_PREDICTION = JobFunctions.appendJobDescriptorAttribute(
            oneTaskBatchJobDescriptor(), JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_VALUE, "12" /* seconds */
    );

    private final TitusStackResource titusStackResource = new TitusStackResource(twoPartitionsPerTierCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.twoPartitionsPerTierStackActivation());
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void noOpportunisticCpusAvailable() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void allOpportunisticCpusAvailable() throws Exception {
        Optional<SimulatedTitusAgentCluster> flexCluster = titusStackResource.getMaster().getSimulatedCloud().getAgentInstanceGroups().stream()
                .filter(cluster -> cluster.getName().startsWith("flex"))
                .findAny();
        assertThat(flexCluster).isPresent();
        List<SimulatedTitusAgent> agents = flexCluster.get().getAgents();
        assertThat(agents).isNotEmpty();

        String allocationId = UUID.randomUUID().toString();
        String agentId = agents.get(agents.size() - 1).getId();
        titusStackResource.getMaster().addOpportunisticCpu(agentId,
                new OpportunisticCpuAvailability(allocationId, Instant.now().plus(Duration.ofHours(6)), 4)
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, agentId)
                        .expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "4")
                ))
                // opportunistic CPUs have been claimed, next task can't use it
                .schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                        .template(ScenarioTemplates.launchJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                                .expectTaskOnAgent()
                                .assertTask(task -> !task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                                !task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                        "Not scheduled on opportunistic CPUs")
                        )
                );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void someOpportunisticCpusAvailable() throws Exception {
        Optional<SimulatedTitusAgentCluster> flexCluster = titusStackResource.getMaster().getSimulatedCloud().getAgentInstanceGroups().stream()
                .filter(cluster -> cluster.getName().startsWith("flex"))
                .findAny();
        assertThat(flexCluster).isPresent();
        List<SimulatedTitusAgent> agents = flexCluster.get().getAgents();
        assertThat(agents).isNotEmpty();

        String allocationId = UUID.randomUUID().toString();
        String agentId = agents.get(agents.size() - 1).getId();
        // 2 out of 4 CPUs are opportunistic on an agent
        titusStackResource.getMaster().addOpportunisticCpu(agentId,
                new OpportunisticCpuAvailability(allocationId, Instant.now().plus(Duration.ofHours(6)), 2)
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, agentId)
                        .expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                )
        );
    }

}
