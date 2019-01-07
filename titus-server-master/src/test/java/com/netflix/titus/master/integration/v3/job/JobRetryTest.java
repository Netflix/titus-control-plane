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

package com.netflix.titus.master.integration.v3.job;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

@Category(IntegrationTest.class)
public class JobRetryTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder()
            .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
            .withExtensions(BatchJobExt.newBuilder()
                    .withSize(1)
                    .withRuntimeLimitMs(600000)
                    .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(1).build())
                    .build()
            )
            .build();
    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = oneTaskServiceJobDescriptor().toBuilder()
            .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
            .withExtensions(ServiceJobExt.newBuilder()
                    .withCapacity(Capacity.newBuilder().withMin(0).withDesired(1).withMax(2).build())
                    .withRetryPolicy(JobModel.newImmediateRetryPolicy().withRetries(1).build())
                    .withServiceJobProcesses(ServiceJobProcesses.newBuilder().build())
                    .build()
            )
            .build();

    private static final TitusStackResource titusStackResource = new TitusStackResource(basicCell(5));

    private static final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @BeforeClass
    public static void setUp() {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
    }

    @Test(timeout = 30_000)
    public void testBatchJobRetry() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = ONE_TASK_BATCH_JOB.toBuilder().withApplicationName("testBatchJobRetry").build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .inTask(0, TaskScenarioBuilder::failTaskExecution)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Finished))
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .assertTasks(task -> task.get(0).getResubmitNumber() == 1)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.startTask()))
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.completeTask()))
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testServiceJobRetry() throws Exception {
        JobDescriptor<ServiceJobExt> jobDescriptor = ONE_TASK_SERVICE_JOB.toBuilder().withApplicationName("testServiceJobRetry").build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.transitionUntil(TaskState.Finished))
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Finished))
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .assertTasks(task -> task.get(0).getResubmitNumber() == 1)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.startTask()))
                .killJob()
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testBatchJobFailsAfterRetrying() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = ONE_TASK_BATCH_JOB.toBuilder().withApplicationName("testBatchJobFailsAfterRetrying").build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .inTask(0, TaskScenarioBuilder::failTaskExecution)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Finished))
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .inTask(0, TaskScenarioBuilder::failTaskExecution)
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void testServiceJobFailsAfterRetrying() throws Exception {
        JobDescriptor<ServiceJobExt> jobDescriptor = ONE_TASK_SERVICE_JOB.toBuilder().withApplicationName("testServiceJobFailsAfterRetrying").build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .inTask(0, TaskScenarioBuilder::failTaskExecution)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Finished))
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .inTask(0, TaskScenarioBuilder::failTaskExecution)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Finished))
                .expectAllTasksCreated() // Service job retries forever
                .killJob()
                .expectJobEventStreamCompletes()
        );
    }
}
