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

package com.netflix.titus.master.integration.v3.job.other;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicKubeCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobScalingTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(basicKubeCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(jobsScenarioBuilder);

    @After
    public void tearDown() throws Exception {
        jobsScenarioBuilder.expectVersionsOrdered();
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(1).withMax(5).build())
                .expectJobToScaleDown()
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJobDesired() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacityDesired(4, 0, 5)
                .expectJobToScaleDown()
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJobMin() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacityMin(2, 5, 2)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJobMinAdjustsDesired() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(1).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacityMin(2, 5, 2)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJobMax() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacityMax(4, 0, 2)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJobMaxAdjustsDesired() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(4).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacityMax(2, 0, 2)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testScaleUpAndDownServiceJobDesiredInvalid() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacityDesiredInvalid(6, 2)
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testTerminateAndShrink() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testTerminateAndShrink"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(2).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .killTaskAndShrink()
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Finished)
                )
                .expectJobUpdateEvent(job -> {
                    Capacity capacity = ((Job<ServiceJobExt>) (Job<?>) job).getJobDescriptor().getExtensions().getCapacity();
                    return capacity.getMin() == 1 && capacity.getDesired() == 1;
                }, "Expected job to scale down to one instance")
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testTerminateAndShrinkNotAllowedIfDesiredToLowAndCheckEnabled() {
        try {
            jobsScenarioBuilder.schedule(newJob("testTerminateAndShrinkNotAllowed"), jobScenarioBuilder -> jobScenarioBuilder
                    .template(ScenarioTemplates.startTasksInNewJob())
                    .updateJobCapacity(JobModel.newCapacity().withMin(2).withDesired(2).withMax(5).build())
                    .expectAllTasksCreated()
                    .inTask(0, TaskScenarioBuilder::killTaskAndShrinkWithMinCheck)
            );
        } catch (Exception e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertThat(cause.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(cause.getMessage()).contains("Terminate and shrink would make desired job size go below the configured minimum");
        }
    }

    private JobDescriptor<ServiceJobExt> newJob(String detail) {
        return oneTaskServiceJobDescriptor().toBuilder()
                .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
                .withJobGroupInfo(JobGroupInfo.newBuilder().withDetail(detail).build())
                .build();
    }

}
