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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

@Category(IntegrationTest.class)
public class JobScalingTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
    }

    @Test
    public void testScaleUpAndDownServiceJob() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testScaleUpAndDownServiceJob"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(1).withMax(5).build())
                .expectJobToScaleDown()
        );
    }

    @Test
    public void testTerminateAndShrink() throws Exception {
        jobsScenarioBuilder.schedule(newJob("testTerminateAndShrink"), jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .updateJobCapacity(JobModel.newCapacity().withMin(0).withDesired(2).withMax(5).build())
                .expectAllTasksCreated()
                .inTask(0, taskScenarioBuilder -> taskScenarioBuilder
                        .killTaskAndShrink()
                        .expectStateUpdateSkipOther(TaskStatus.TaskState.Finished)
                )
                .expectJobUpdateEvent(job -> hasSize(job, 1), "Expected job to scale down to one instance")
        );
    }

    private JobDescriptor<ServiceJobExt> newJob(String detail) {
        return oneTaskServiceJobDescriptor().toBuilder()
                .withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX)
                .withJobGroupInfo(JobGroupInfo.newBuilder().withDetail(detail).build())
                .build();
    }

    private static boolean hasSize(Job<?> job, int expected) {
        Job<ServiceJobExt> serviceJob = (Job<ServiceJobExt>) job;
        int actual = serviceJob.getJobDescriptor().getExtensions().getCapacity().getDesired();
        return actual == expected;
    }
}
