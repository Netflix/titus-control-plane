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
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

@Category(IntegrationTest.class)
public class JobScalingTest {

    private static final JobDescriptor<ServiceJobExt> ONE_TASK_SERVICE_JOB = oneTaskServiceJobDescriptor().toBuilder()
            .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
            .build();

    @Rule
    public final TitusStackResource titusStackResource = TitusStackResource.aDefaultStack();

    private JobsScenarioBuilder jobsScenarioBuilder;

    @Before
    public void setUp() throws Exception {
        jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource.getStack().getTitusOperations());
    }

    @Test
    public void testScaleUpAndDownServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(1).withMax(5).build())
                .expectJobToScaleDown()
        );
    }

    @Test
    public void testTerminateAndShrink() throws Exception {
        jobsScenarioBuilder.schedule(ONE_TASK_SERVICE_JOB, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .killTaskAndShrink()
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Finished)
                )
                .expectJobUpdateEvent(job -> hasSize(job, 1), "Expected job to scale down to one instance")
        );
    }

    private boolean hasSize(Job<?> job, int expected) {
        Job<ServiceJobExt> serviceJob = (Job<ServiceJobExt>) job;
        int actual = serviceJob.getJobDescriptor().getExtensions().getCapacity().getDesired();
        return actual == expected;
    }
}
