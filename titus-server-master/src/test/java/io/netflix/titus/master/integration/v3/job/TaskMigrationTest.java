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

package io.netflix.titus.master.integration.v3.job;

import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.master.integration.BaseIntegrationTest;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.activate;
import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.evacuate;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.moveToState;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startV2TasksInNewJob;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioUtil.baseServiceJobDescriptor;
import static io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks.twoPartitionsPerTierStack;

@Category(IntegrationTest.class)
public class TaskMigrationTest extends BaseIntegrationTest {

    private static final TitusStackResource titusStackResource = new TitusStackResource(twoPartitionsPerTierStack(2));

    private static final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @BeforeClass
    public static void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud();
    }

    @Test(timeout = 30_000)
    public void migrateV2ServiceJob() throws Exception {
        testMigration(true);
    }

    @Test(timeout = 30_000)
    public void migrateV3ServiceJob() throws Exception {
        testMigration(false);
    }

    private void testMigration(boolean v2Mode) throws Exception {
        instanceGroupsScenarioBuilder.template(activate("flex1")).template(evacuate("flex2"));
        JobDescriptor<ServiceJobExt> jobDescriptor = baseServiceJobDescriptor(v2Mode).build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                // Run task on instance group 'flex1'
                .template(startV2TasksInNewJob())
                .assertEachTask(task -> task.getTaskContext().get("agent.itype").equals("m3.2xlarge"), "Task should be on instance group flex1")
                // Migrate to instance group 'flex2'
                .andThen(() -> instanceGroupsScenarioBuilder.template(evacuate("flex1")).template(activate("flex2")))
                .expectTaskInSlot(0, 1)
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .allTasks(moveToState(TaskStatus.TaskState.Started))
                .assertEachTask(task -> task.getTaskContext().get("agent.itype").equals("m4.2xlarge"), "Task should be on instance group flex1")
        );
    }
}
