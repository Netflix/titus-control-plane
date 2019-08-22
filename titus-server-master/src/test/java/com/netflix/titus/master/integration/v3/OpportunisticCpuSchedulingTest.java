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
import java.util.UUID;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_AGENT_ASG;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.twoPartitionsPerTierCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

@Category(IntegrationTest.class)
public class OpportunisticCpuSchedulingTest extends BaseIntegrationTest {
    private static final JobDescriptor<BatchJobExt> BATCH_JOB_WITH_RUNTIME_PREDICTION = JobFunctions.appendJobDescriptorAttribute(
            oneTaskBatchJobDescriptor(), JobAttributes.JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC, "12" /* seconds */
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

    /**
     * When no opportunistic CPUs are available in the system, the task should eventually be scheduled consuming only
     * regular CPUs
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void noOpportunisticCpusAvailable() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void availabilityNotLongEnough() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofSeconds(10));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex2",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        // job runtime is 12s, but opportunistic cpus are available for 10s
        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void allOpportunisticCpusAvailable() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex2",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex2")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "4")
                )
                .template(ScenarioTemplates.startLaunchedTasks())) // free up launch guard

                // opportunistic CPUs have been claimed, next task can't use it
                .schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                        .template(ScenarioTemplates.launchJob())
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                                .expectTaskOnAgent()
                                .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                                !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                        "Not scheduled on opportunistic CPUs")
                        )
                );

        // free up opportunistic CPUs, window is still valid
        jobsScenarioBuilder.takeJob(0).template(ScenarioTemplates.killJob());

        // opportunistic CPUs are available again, and window is still valid (6h expiration)
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex2")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "4")
                ));
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void expiredOpportunisticCpuAvailability() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().minus(Duration.ofMillis(1));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 10);
        instanceGroupsScenarioBuilder.apply("flex2",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void someOpportunisticCpusAvailable() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 2);
        instanceGroupsScenarioBuilder.apply("flex2",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex2")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void opportunisticSchedulingCanBeDisabled() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex2",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder()
                        .withCpu(2)
                        .withMemoryMB(1)
                        .withDiskMB(1)
                        .withNetworkMbps(1)
                        .withGpu(0)
                )
        );
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex2")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                )
                .template(ScenarioTemplates.startLaunchedTasks()) // free up launch guard
        );

        titusStackResource.getMaster().updateProperty("titus.feature.opportunisticResourcesSchedulingEnabled", "false");
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                )
                .template(ScenarioTemplates.startLaunchedTasks()) // free up launch guard
        );

        titusStackResource.getMaster().updateProperty("titus.feature.opportunisticResourcesSchedulingEnabled", "true");
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex2")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                )
                .template(ScenarioTemplates.startLaunchedTasks()) // free up launch guard
        );

    }
}
