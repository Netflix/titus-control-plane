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

package com.netflix.titus.master.integration.v3;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobAsserts;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.twoPartitionsPerTierCell;
import static com.netflix.titus.testkit.junit.master.TitusStackResource.V3_ENGINE_APP_PREFIX;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

@Category(IntegrationTest.class)
public class V3JobSchedulingTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder().withApplicationName(V3_ENGINE_APP_PREFIX).build();

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
     * Verify batch job submission for two agent clusters with identical fitness, but only one having required
     * resources.
     * TODO We should add second cluster in this test, but as adding cluster requires master restart, we provide two clusters in the initialization step
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void submitBatchJobWhenTwoAgentClustersWithSameFitnessButDifferentResourceAmounts() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor =
                ONE_TASK_BATCH_JOB.but(j -> j.getContainer().but(c -> c.getContainerResources().toBuilder().withCpu(7)));

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectInstanceType(AwsInstanceType.M3_2XLARGE))
        );
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void submitBatchJobAndRebootTitusMaster() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
        );

        jobsScenarioBuilder.stop();
        titusStackResource.getMaster().reboot();

        JobsScenarioBuilder newJobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource.getOperations());
        newJobsScenarioBuilder
                .assertJobs(jobs -> jobs.size() == 1)
                .takeJob(0)
                .assertJob(JobAsserts.jobInState(JobState.Accepted))
                .assertTasks(tasks -> tasks.size() == 1);
    }
}
