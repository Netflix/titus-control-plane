/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.job.basic;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicKubeCell;
import static com.netflix.titus.testkit.junit.master.TitusStackResource.V3_ENGINE_APP_PREFIX;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class TaskMoveTest extends BaseIntegrationTest {

    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = oneTaskServiceJobDescriptor().toBuilder().withApplicationName(V3_ENGINE_APP_PREFIX).build();

    private final TitusStackResource titusStackResource = new TitusStackResource(basicKubeCell(2).toMaster(master ->
            master.withProperty("titus.feature.moveTaskApiEnabled", "true")
    ), true);

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(jobsScenarioBuilder);

    @After
    public void tearDown() throws Exception {
        jobsScenarioBuilder.expectVersionsOrdered();
    }

    @Test(timeout = 30_000)
    public void testSubmitSimpleServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, 2, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
        );
        String targetJobId = jobsScenarioBuilder.takeJob(0).getJobId();
        String sourceJobId = jobsScenarioBuilder.takeJob(1).getJobId();

        jobsScenarioBuilder.takeJob(1)
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder.moveTask(targetJobId))
                .expectJobUpdateEvent(job -> JobFunctions.getJobDesiredSize(job) == 0, "Job with no tasks expected");

        jobsScenarioBuilder.takeJob(0)
                .expectJobUpdateEvent(job -> JobFunctions.getJobDesiredSize(job) == 2, "Job with two tasks expected")
                .inTask(1, taskScenarioBuilder -> taskScenarioBuilder
                        .assertTaskUpdate(task -> {
                            assertThat(task.getJobId()).isEqualTo(targetJobId);
                            assertThat(task.getTaskContext()).containsEntry(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB, sourceJobId);
                        })
                );
    }
}
