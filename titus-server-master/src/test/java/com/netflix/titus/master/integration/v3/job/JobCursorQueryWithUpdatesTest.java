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

import java.util.List;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.basicSetupActivation;
import static com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks.basicStack;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobCursorQueryWithUpdatesTest extends BaseIntegrationTest {
    private static final int JOBS_PER_ENGINE = 3;
    private static final int TASKS_PER_JOB = 2;

    private final TitusStackResource titusStackResource = new TitusStackResource(basicStack(5));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    private List<Job> allJobsInOrder;
    private List<Task> allTasksInOrder;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicSetupActivation());
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();

        JobDescriptor<ServiceJobExt> v2App = JobDescriptorGenerator.oneTaskServiceJobDescriptor()
                .but(jd -> jd.toBuilder().withApplicationName(TitusStackResource.V2_ENGINE_APP_PREFIX).build())
                .but(jd -> jd.getExtensions().toBuilder().withCapacity(
                        Capacity.newBuilder().withMin(0).withDesired(TASKS_PER_JOB).withMax(TASKS_PER_JOB).build()
                ).build());

        JobDescriptor<ServiceJobExt> v3App = v2App.but(jd ->
                jd.toBuilder().withApplicationName(TitusStackResource.V3_ENGINE_APP_PREFIX).build()
        );

        jobsScenarioBuilder.schedule(v2App, JOBS_PER_ENGINE, ScenarioTemplates.startTasksInNewJob());
        jobsScenarioBuilder.schedule(v3App, JOBS_PER_ENGINE, ScenarioTemplates.startTasksInNewJob());

        this.allJobsInOrder = client.findJobs(JobQuery.newBuilder().setPage(Page.newBuilder().setPageSize(Integer.MAX_VALUE / 2)).build()).getItemsList();
        assertThat(allJobsInOrder).hasSize(2 * JOBS_PER_ENGINE);

        this.allTasksInOrder = client.findTasks(TaskQuery.newBuilder().setPage(Page.newBuilder().setPageSize(Integer.MAX_VALUE / 2)).build()).getItemsList();
        assertThat(allTasksInOrder).hasSize(2 * JOBS_PER_ENGINE * TASKS_PER_JOB);
    }

    @Test
    public void testJobQueryWithRemovedItems() {
        // Page 0
        JobQueryResult result0 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2)).build()
        );
        assertThat(result0.getItemsList()).containsExactlyElementsOf(allJobsInOrder.subList(0, 2));

        // Remove item at the cursor position
        jobsScenarioBuilder.takeJob(result0.getItems(1).getId())
                .onV2Template(ScenarioTemplates.killV2Job())
                .onV3Template(ScenarioTemplates.killJob());
        JobQueryResult result1 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2).setCursor(result0.getPagination().getCursor())).build()
        );
        assertThat(result1.getItemsList()).containsExactlyElementsOf(allJobsInOrder.subList(2, 4));

        // Remove last items
        jobsScenarioBuilder.takeJob(allJobsInOrder.get(4).getId())
                .onV2Template(ScenarioTemplates.killV2Job())
                .onV3Template(ScenarioTemplates.killJob());
        jobsScenarioBuilder.takeJob(allJobsInOrder.get(5).getId())
                .onV2Template(ScenarioTemplates.killV2Job())
                .onV3Template(ScenarioTemplates.killJob());

        JobQueryResult result2 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2).setCursor(result1.getPagination().getCursor())).build()
        );
        assertThat(result2.getItemsList()).isEmpty();
    }

    @Test
    public void testTaskQueryWithRemovedItems() {
        // Page 0
        TaskQueryResult result0 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4)).build()
        );
        assertThat(result0.getItemsList()).containsExactlyElementsOf(allTasksInOrder.subList(0, 4));

        // Remove item at the cursor position
        jobsScenarioBuilder.takeJob(result0.getItems(3).getJobId())
                .onV2Template(jb -> jb.getTask(result0.getItems(3).getId()).template(ScenarioTemplates.terminateAndShrinkV2()).toJob())
                .onV3Template(jb -> jb.getTask(result0.getItems(3).getId()).template(ScenarioTemplates.terminateAndShrinkV3()).toJob());
        TaskQueryResult result1 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4).setCursor(result0.getPagination().getCursor())).build()
        );
        assertThat(result1.getItemsList()).containsExactlyElementsOf(allTasksInOrder.subList(4, 8));

        // Remove last items
        jobsScenarioBuilder.takeJob(allTasksInOrder.get(10).getJobId())
                .onV2Template(jb -> jb.getTask(allTasksInOrder.get(10).getId()).template(ScenarioTemplates.terminateAndShrinkV2()).toJob())
                .onV3Template(jb -> jb.getTask(allTasksInOrder.get(10).getId()).template(ScenarioTemplates.terminateAndShrinkV3()).toJob());
        jobsScenarioBuilder.takeJob(allTasksInOrder.get(11).getJobId())
                .onV2Template(jb -> jb.getTask(allTasksInOrder.get(11).getId()).template(ScenarioTemplates.terminateAndShrinkV2()).toJob())
                .onV3Template(jb -> jb.getTask(allTasksInOrder.get(11).getId()).template(ScenarioTemplates.terminateAndShrinkV3()).toJob());

        TaskQueryResult result2 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2).setCursor(result1.getPagination().getCursor())).build()
        );
        assertThat(result2.getItemsList()).containsExactlyElementsOf(allTasksInOrder.subList(8, 10));
    }
}
