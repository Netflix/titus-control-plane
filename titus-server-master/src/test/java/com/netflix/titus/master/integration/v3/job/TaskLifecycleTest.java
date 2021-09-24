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

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicKubeCell;
import static com.netflix.titus.testkit.junit.master.TitusStackResource.V3_ENGINE_APP_PREFIX;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

/**
 * 'Launched' state is no longer tested, as it is not used in the Kube integration (we fill in a dummy value).
 */
@Category(IntegrationTest.class)
public class TaskLifecycleTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> ONE_TASK_BATCH_JOB = oneTaskBatchJobDescriptor().toBuilder().withApplicationName(V3_ENGINE_APP_PREFIX).build();

    private final TitusStackResource titusStackResource = new TitusStackResource(basicKubeCell(2).toMaster(master -> master
            .withProperty("titusMaster.jobManager.taskInLaunchedStateTimeoutMs", "2000")
            .withProperty("titusMaster.jobManager.batchTaskInStartInitiatedStateTimeoutMs", "2000")
            .withProperty("titusMaster.jobManager.serviceTaskInStartInitiatedStateTimeoutMs", "2000")
            .withProperty("titusMaster.jobManager.taskInKillInitiatedStateTimeoutMs", "100")
    ));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(jobsScenarioBuilder);

    @After
    public void tearDown() throws Exception {
        jobsScenarioBuilder.expectVersionsOrdered();
    }

    @Test(timeout = 30_000)
    public void submitBatchTaskStuckInStartInitiated() {
        testTaskStuckInState(ONE_TASK_BATCH_JOB, TaskState.StartInitiated);
    }

    @Test(timeout = 30_000)
    public void submitBatchJobStuckInKillInitiated() {
        jobsScenarioBuilder.schedule(ONE_TASK_BATCH_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startJobAndMoveTasksToKillInitiated())
                .expectJobEventStreamCompletes()
        );
    }

    @Test(timeout = 30_000)
    public void submitServiceTaskStuckInStartInitiated() {
        testTaskStuckInState(newJob("submitServiceJobStuckInStartInitiated"), TaskState.StartInitiated);
    }

    @Test(timeout = 30_000)
    public void submitServiceJobStuckInKillInitiated() {
        jobsScenarioBuilder.schedule(newJob("submitServiceJobStuckInKillInitiated"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startJobAndMoveTasksToKillInitiated())
                .expectTaskInSlot(0, 1)
        );
    }

    private void testTaskStuckInState(JobDescriptor<?> jobDescriptor, TaskState state) {
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectAllTasksInKube)
                .schedule()
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.template(ScenarioTemplates.lockTaskInState(state)))
        );
    }

    private JobDescriptor<ServiceJobExt> newJob(String detail) {
        return oneTaskServiceJobDescriptor().toBuilder()
                .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
                .withJobGroupInfo(JobGroupInfo.newBuilder().withDetail(detail).build())
                .build();
    }
}
