/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.integration.v3.agent;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.master.integration.BaseIntegrationTest;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.activate;
import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.deactivate;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks.twoPartitionsPerTierStack;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

/**
 * A collection of tests verifying that configured agent instance group placement constraints (tier, lifecycle state)
 * are enforced.
 */
@Category(IntegrationTest.class)
public class AgentPlacementConstraintTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder().withApplicationName("myApp").build();

    private final TitusStackResource titusStackResource = new TitusStackResource(twoPartitionsPerTierStack(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain chain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud();
    }

    /**
     * Having two clusters in flex tier, disable second and verify that job is scheduled on the first one.
     * Next disable first, and enable second, and verify that scheduled job runs on the second cluster.
     */
    @Test(timeout = 30_000)
    public void scheduleJobOnActiveAgentCluster() throws Exception {
        // Activate 'flex1' only
        instanceGroupsScenarioBuilder.template(activate("flex1"));

        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .assertEachTask(task -> task.getTaskContext().get("agent.itype").equals("m3.2xlarge"), "Task placed on bad instance type")
                .killJob()
        );

        // Activate 'flex2' only
        instanceGroupsScenarioBuilder.template(deactivate("flex1")).template(activate("flex2"));

        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .assertEachTask(task -> task.getTaskContext().get("agent.itype").equals("m4.2xlarge"), "Task placed on bad instance type")
                .killJob()
        );
    }
}
