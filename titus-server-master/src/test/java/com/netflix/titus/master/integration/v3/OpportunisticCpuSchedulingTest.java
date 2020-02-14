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

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.runtime.endpoint.admission.JobRuntimePredictionConfiguration;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
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
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.jobAccepted;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.killJob;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.launchJob;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startJob;
import static com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startLaunchedTask;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.cellWithRuntimePredictions;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

@Category(IntegrationTest.class)
public class OpportunisticCpuSchedulingTest extends BaseIntegrationTest {
    private static final JobDescriptor<BatchJobExt> BATCH_JOB_WITH_RUNTIME_PREDICTION = oneTaskBatchJobDescriptor().but(
            // runtime predictions are automatically set (and capped) to the runtime limit
            jd -> jd.getExtensions().toBuilder().withRuntimeLimitMs(60_000L)
    );

    private final JobRuntimePredictionConfiguration runtimePredictionConfig = () -> 300_000L;
    private final TitusStackResource titusStackResource = new TitusStackResource(cellWithRuntimePredictions(EmbeddedTitusMaster.CELL_NAME, 2, runtimePredictionConfig));
    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);
    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
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
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                        .template(startLaunchedTask())
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void availabilityNotLongEnough() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofSeconds(45));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex1",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        // job runtime is 12s, but opportunistic cpus are available for 10s
        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                        .template(startLaunchedTask())
                )
        );
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void allOpportunisticCpusAvailable() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex1",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(2))
        );

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                        // free up launch guard
                        .template(startLaunchedTask())
                ));

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                        // free up launch guard
                        .template(startLaunchedTask())
                ));

        // all opportunistic CPUs have been claimed, next task can't use it
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                        // free up launch guard
                        .template(startLaunchedTask())
                )
        );

        // free up opportunistic CPUs, window is still valid
        jobsScenarioBuilder.takeJob(0).template(killJob());

        // opportunistic CPUs are available again, and window is still valid (6h expiration)
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                        // free up launch guard
                        .template(startLaunchedTask())
                ));
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void expiredOpportunisticCpuAvailability() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().minus(Duration.ofMillis(1));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 10);
        instanceGroupsScenarioBuilder.apply("flex1",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                        .template(startLaunchedTask())
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void someOpportunisticCpusAvailable() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 2);
        instanceGroupsScenarioBuilder.apply("flex1",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                        .template(startLaunchedTask())
                )
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void launchGuardsDoNotPreventOpportunisticScheduling() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        titusStackResource.getMaster().getSimulatedCloud().updateAgentGroupCapacity("flex1", 1, 1, 1);
        instanceGroupsScenarioBuilder.apply("flex1", group -> group
                .awaitDesiredSize(1)
                .awaitDesiredInstanceCount() // ensure the agent is scaled down
                .any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> regularJob = oneTaskBatchJobDescriptor().but(
                // 10min > config.opportunisticRuntimeLimit (5min)
                j -> j.getExtensions().toBuilder().withRuntimeLimitMs(10 * 60 * 1000)
        );
        JobDescriptor<BatchJobExt> opportunisticJob = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        String regularJobId = jobsScenarioBuilder.schedule(regularJob,
                // move to StartInitiated to prevent "stuck in Launched" errors
                jobScenarioBuilder -> jobScenarioBuilder.template(startJob(TaskStatus.TaskState.StartInitiated))
                        .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                                .expectTaskOnAgent() /* potentially hold launch guard */
                                .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                                .expectNoTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT)
                        )
        ).takeJobId(0);

        String opportunisticJobId = jobsScenarioBuilder.schedule(opportunisticJob,
                jobScenarioBuilder -> jobScenarioBuilder.template(jobAccepted()).expectAllTasksCreated()
        ).takeJobId(1);

        // free up potential launch guards
        jobsScenarioBuilder.takeJob(regularJobId).template(jobScenarioBuilder ->
                jobScenarioBuilder.allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .transitionUntil(TaskStatus.TaskState.Started)
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Started)
                )
        );

        // opportunistic scheduling can now proceed without decrementing allocated opportunistic CPUs
        jobsScenarioBuilder.takeJob(opportunisticJobId)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Launched)
                        .template(startLaunchedTask())
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "4")
                );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void allAgentsNonSchedulableDoesNotPreventOpportunisticScheduling() throws Exception {
        // all schedulable agents are deactivated, only GPU instances are remaining
        instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.deactivate("flex1", "critical1"));

        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex1",
                group -> group.any(instance -> instance.addOpportunisticCpus(availability))
        );

        JobDescriptor<BatchJobExt> opportunisticJob = BATCH_JOB_WITH_RUNTIME_PREDICTION.but(j ->
                j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(4))
        );

        String opportunisticJobId = jobsScenarioBuilder.schedule(opportunisticJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(jobAccepted())
                .expectAllTasksCreated()
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectSchedulingFailed()
                        .assertTask(task -> task.getStatus().getState().equals(TaskState.Accepted), "are still waiting for schedulable agents")
                )
        ).takeJobId(0);

        // activate schedulable agents
        instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex1"));

        // opportunistic scheduling can now proceed without decrementing allocated opportunistic CPUs
        jobsScenarioBuilder.takeJob(opportunisticJobId)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Launched)
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "4")
                );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void opportunisticSchedulingCanBeDisabled() throws Exception {
        String allocationId = UUID.randomUUID().toString();
        Instant expiresAt = Instant.now().plus(Duration.ofHours(6));
        OpportunisticCpuAvailability availability = new OpportunisticCpuAvailability(allocationId, expiresAt, 4);
        instanceGroupsScenarioBuilder.apply("flex1",
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
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                        // free up launch guard
                        .template(startLaunchedTask())
                ));

        titusStackResource.getMaster().updateProperty("titus.feature.opportunisticResourcesSchedulingEnabled", "false");
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .assertTask(task -> !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION) &&
                                        !task.getTaskContext().containsKey(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT),
                                "Not scheduled on opportunistic CPUs")
                        // free up launch guard
                        .template(startLaunchedTask())
                ));

        titusStackResource.getMaster().updateProperty("titus.feature.opportunisticResourcesSchedulingEnabled", "true");
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(launchJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .expectTaskOnAgent()
                        .expectTaskContext(TASK_ATTRIBUTES_AGENT_ASG, "flex1")
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_ALLOCATION, allocationId)
                        .expectTaskContext(TASK_ATTRIBUTES_OPPORTUNISTIC_CPU_COUNT, "2")
                        // free up launch guard
                        .template(startLaunchedTask())
                ));
    }
}
