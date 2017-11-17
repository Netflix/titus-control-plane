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

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.completeTask;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startV2TasksInNewJob;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioUtil.baseBatchJobDescriptor;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioUtil.baseServiceJobDescriptor;
import static io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks.basicStack;
import static org.assertj.core.api.Assertions.assertThat;


@Category(IntegrationTest.class)
public class JobDirectQueryTest {
    private static final String NON_EXISTING_V2_ID = "Titus-non_existing_id";
    private static final String NON_EXISTING_V3_ID = "non_existing_id";

    private static final TitusStackResource titusStackResource = new TitusStackResource(basicStack(2));

    private static final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private static JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    private static String v2BatchJobId;
    private static String v2BatchTaskId;
    private static String v3BatchJobId;
    private static String v3BatchTaskId;
    private static String v3ArchivedBatchJobId;
    private static String v3ArchivedBatchTaskId;

    private static String v2ServiceJobId;
    private static String v2ServiceTaskId;
    private static String v3ServiceJobId;

    @BeforeClass
    public static void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud();

        client = titusStackResource.getGateway().getV3BlockingGrpcClient();

        // Batch Jobs
        jobsScenarioBuilder.schedule(
                baseBatchJobDescriptor(true).build(),
                jobScenarioBuilder -> jobScenarioBuilder.template(startV2TasksInNewJob())
        );
        jobsScenarioBuilder.schedule(
                baseBatchJobDescriptor(false).build(),
                jobScenarioBuilder -> jobScenarioBuilder.template(startTasksInNewJob())
        );
        jobsScenarioBuilder.schedule(
                baseBatchJobDescriptor(false).build(),
                jobScenarioBuilder -> jobScenarioBuilder.template(startTasksInNewJob())
                        .allTasks(completeTask())
                        .killJob()
        );

        //Service Jobs
        jobsScenarioBuilder.schedule(
                baseServiceJobDescriptor(true).build(),
                jobScenarioBuilder -> jobScenarioBuilder.template(startV2TasksInNewJob())
        );
        jobsScenarioBuilder.schedule(
                baseServiceJobDescriptor(false).build(),
                jobScenarioBuilder -> jobScenarioBuilder.template(startTasksInNewJob())
        );

        v2BatchJobId = jobsScenarioBuilder.takeJobId(0);
        v2BatchTaskId = jobsScenarioBuilder.takeTaskId(0, 0);
        v3BatchJobId = jobsScenarioBuilder.takeJobId(1);
        v3BatchTaskId = jobsScenarioBuilder.takeTaskId(1, 0);
        v3ArchivedBatchJobId = jobsScenarioBuilder.takeJobId(2);
        v3ArchivedBatchTaskId = jobsScenarioBuilder.takeTaskId(2, 0);

        v2ServiceJobId = jobsScenarioBuilder.takeJobId(3);
        v2ServiceTaskId = jobsScenarioBuilder.takeTaskId(3, 0);
        v3ServiceJobId = jobsScenarioBuilder.takeJobId(4);
    }

    @Test(timeout = 30_000)
    public void testFindBatchJobByIdV2() throws Exception {
        testFindBatchJob(v2BatchJobId);
    }

    @Test(timeout = 30_000)
    public void testFindBatchJobByIdV3() throws Exception {
        testFindBatchJob(v3BatchJobId);
    }

    @Test(timeout = 30_000)
    public void testFindArchivedBatchJobByIdV3() throws Exception {
        testFindBatchJob(v3ArchivedBatchJobId);
    }

    private void testFindBatchJob(String jobId) {
        Job job = client.findJob(JobId.newBuilder().setId(jobId).build());
        assertThat(job.getId()).isEqualTo(jobId);
    }

    @Test(timeout = 30_000)
    public void testFindServiceJobByIdV2() throws Exception {
        testFindServiceJob(v2ServiceJobId);
    }

    @Test(timeout = 30_000)
    public void testFindServiceJobByIdV3() throws Exception {
        testFindServiceJob(v3ServiceJobId);
    }

    private void testFindServiceJob(String jobId) {
        Job job = client.findJob(JobId.newBuilder().setId(jobId).build());
        assertThat(job.getId()).isEqualTo(jobId);
        assertThat(job.getJobDescriptor().getContainer().getResources().getAllocateIP()).isTrue();
    }

    @Test(timeout = 30_000)
    public void testFindNonExistingJobByIdV2() throws Exception {
        try {
            client.findJob(JobId.newBuilder().setId(NON_EXISTING_V2_ID).build());
        } catch (Exception e) {
            assertThat(e.getMessage()).contains(NON_EXISTING_V2_ID);
        }
    }

    @Test(timeout = 30_000)
    public void testFindNonExistingJobByIdV3() throws Exception {
        try {
            client.findJob(JobId.newBuilder().setId(NON_EXISTING_V3_ID).build());
        } catch (Exception e) {
            assertThat(e.getMessage()).contains(NON_EXISTING_V3_ID);
        }
    }

    @Test(timeout = 30_000)
    public void testFindBatchTaskByIdV2() throws Exception {
        Task task = client.findTask(TaskId.newBuilder().setId(v2BatchTaskId).build());
        assertThat(task.getId()).isEqualTo(v2BatchTaskId);
    }

    @Test(timeout = 30_000)
    public void testFindServiceTaskByIdV2() throws Exception {
        Task task = client.findTask(TaskId.newBuilder().setId(v2ServiceTaskId).build());
        assertThat(task.getId()).isEqualTo(v2ServiceTaskId);
        assertThat(task.getTaskContextMap().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)).isNotEmpty();
    }

    @Test(timeout = 30_000)
    public void testFindNonExistingTaskByIdV2() throws Exception {
        try {
            client.findTask(TaskId.newBuilder().setId(NON_EXISTING_V2_ID).build());
        } catch (Exception e) {
            assertThat(e.getMessage()).contains(NON_EXISTING_V2_ID);
        }
    }

    @Test(timeout = 30_000)
    public void testFindTaskByIdV3() throws Exception {
        Task task = client.findTask(TaskId.newBuilder().setId(v3BatchTaskId).build());
        assertThat(task.getId()).isEqualTo(v3BatchTaskId);
    }

    @Test(timeout = 30_000)
    public void testFindArchivedTaskByIdV3() throws Exception {
        Task task = client.findTask(TaskId.newBuilder().setId(v3ArchivedBatchTaskId).build());
        assertThat(task.getId()).isEqualTo(v3ArchivedBatchTaskId);
    }

    @Test(timeout = 30_000)
    public void testFindNonExistingTaskByIdV3() throws Exception {
        try {
            client.findTask(TaskId.newBuilder().setId(NON_EXISTING_V3_ID).build());
        } catch (Exception e) {
            assertThat(e.getMessage()).contains(NON_EXISTING_V3_ID);
        }
    }
}
