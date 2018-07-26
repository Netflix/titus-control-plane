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

package com.netflix.titus.master.integration;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cloud.SimulatedClouds.twoPartitionsPerTierStack;
import static com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters.basicMaster;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

/**
 * A collection of integration tests that test task migration.
 */
@Category(IntegrationTest.class)
public class TaskMigrationTest extends BaseIntegrationTest {

    private final TitusMasterResource titusMasterResource = new TitusMasterResource(basicMaster(twoPartitionsPerTierStack(3)));

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusMasterResource);

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusMasterResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusMasterResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud();
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void doNotMigrateBatchJob() throws Exception {
        instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex1"));

        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startJob(TaskStatus.TaskState.Started))
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .expectInstanceType(AwsInstanceType.M3_2XLARGE)
                )
                .andThen(() -> {
                    instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex2"));
                    // We do not have notification mechanism, so we need to wait
                    ExceptionExt.silent(() -> Thread.sleep(20_000));
                })
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .assertTask(task -> task.getResubmitNumber() == 0, "Task resubmitted")
                )
        );
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void migrateServiceJob() throws Exception {
        instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex1"));

        jobsScenarioBuilder.schedule(oneTaskServiceJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startJob(TaskStatus.TaskState.Started))
                .assertEachTask(task -> task.getTaskContext().get("agent.asg").equals("flex1"), "Task should be on instance group flex1")
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .expectInstanceType(AwsInstanceType.M3_2XLARGE)
                        .andThen(() -> {
                            instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.deactivate("flex1"));
                            instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex2"));
                        })
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Finished)
                )
                .expectAllTasksCreated()
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .assertTask(task -> task.getResubmitNumber() == 1, "Task not resubmitted")
                )
                .template(ScenarioTemplates.startTasks())
                .assertEachTask(task -> task.getTaskContext().get("agent.asg").equals("flex2"), "Task should be on instance group flex2")
        );
    }

    @Test(timeout = LONG_TEST_TIMEOUT_MS)
    public void migrateServiceJobAndVerifyThatOnlyOneTaskIsMigratedFirst() throws Exception {
        JobDescriptor<ServiceJobExt> twoTaskJob = oneTaskServiceJobDescriptor().but(jd ->
                jd.getExtensions().toBuilder().withCapacity(
                        Capacity.newBuilder().withMin(2).withDesired(2).withMax(2).build())
        );

        instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex1"));

        jobsScenarioBuilder.schedule(twoTaskJob, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startJob(TaskStatus.TaskState.Started))
                .andThen(() -> {
                    instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.deactivate("flex1"));
                    instanceGroupsScenarioBuilder.template(InstanceGroupScenarioTemplates.activate("flex2"));
                })
                .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getResubmitNumber() == 1)
                .expectSome(1, taskScenarioBuilder -> taskScenarioBuilder.getTask().getResubmitNumber() == 0)
        );
    }
}
