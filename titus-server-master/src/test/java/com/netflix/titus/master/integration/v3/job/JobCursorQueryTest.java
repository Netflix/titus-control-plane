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

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.grpc.StatusRuntimeException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobCursorQueryTest extends BaseIntegrationTest {

    private static final int JOBS_PER_ENGINE = 3;
    private static final int TASKS_PER_JOB = 2;

    private static final TitusStackResource titusStackResource = new TitusStackResource(basicCell(5));

    private static final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private static JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    private static List<Job> allJobsInOrder;
    private static List<Task> allTasksInOrder;

    @BeforeClass
    public static void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicSetupActivation());
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();

        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor()
                .but(jd -> jd.getExtensions().toBuilder().withCapacity(
                        Capacity.newBuilder().withMin(0).withDesired(TASKS_PER_JOB).withMax(TASKS_PER_JOB).build()
                ).build());

        jobsScenarioBuilder.schedule(jobDescriptor.toBuilder().withApplicationName("app1").build(), JOBS_PER_ENGINE, ScenarioTemplates.startTasksInNewJob());
        jobsScenarioBuilder.schedule(jobDescriptor.toBuilder().withApplicationName("app2").build(), JOBS_PER_ENGINE, ScenarioTemplates.startTasksInNewJob());

        allJobsInOrder = client.findJobs(JobQuery.newBuilder().setPage(Page.newBuilder().setPageSize(Integer.MAX_VALUE / 2)).build()).getItemsList();
        assertThat(allJobsInOrder).hasSize(2 * JOBS_PER_ENGINE);

        allTasksInOrder = client.findTasks(TaskQuery.newBuilder().setPage(Page.newBuilder().setPageSize(Integer.MAX_VALUE / 2)).build()).getItemsList();
        assertThat(allTasksInOrder).hasSize(2 * JOBS_PER_ENGINE * TASKS_PER_JOB);
    }

    @Test
    public void testJobQueryWithCursor() {
        // Page 0
        JobQueryResult result0 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2)).build()
        );
        assertThat(result0.getItemsList()).containsExactlyElementsOf(allJobsInOrder.subList(0, 2));
        assertThat(result0.getPagination().getCursor()).isNotEmpty();
        assertThat(result0.getPagination().getCursorPosition()).isEqualTo(1);
        assertThat(result0.getPagination().getHasMore()).isTrue();
        assertThat(result0.getPagination().getCurrentPage().getPageNumber()).isEqualTo(0);

        // Page 1
        JobQueryResult result1 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2).setCursor(result0.getPagination().getCursor())).build()
        );
        assertThat(result1.getItemsList()).containsExactlyElementsOf(allJobsInOrder.subList(2, 4));
        assertThat(result1.getPagination().getCursor()).isNotEmpty();
        assertThat(result1.getPagination().getCursorPosition()).isEqualTo(3);
        assertThat(result1.getPagination().getHasMore()).isTrue();
        assertThat(result1.getPagination().getCurrentPage().getPageNumber()).isEqualTo(1);

        // Page 2
        JobQueryResult result2 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2).setCursor(result1.getPagination().getCursor())).build()
        );
        assertThat(result2.getItemsList()).containsExactlyElementsOf(allJobsInOrder.subList(4, 6));
        assertThat(result2.getPagination().getCursor()).isNotEmpty();
        assertThat(result2.getPagination().getCursorPosition()).isEqualTo(5);
        assertThat(result2.getPagination().getHasMore()).isFalse();
        assertThat(result2.getPagination().getCurrentPage().getPageNumber()).isEqualTo(2);

        // Check cursor points to the latest returned element
        JobQueryResult result3 = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(2).setCursor(result2.getPagination().getCursor())).build()
        );
        assertThat(result3.getItemsList()).isEmpty();
        assertThat(result3.getPagination().getCursor()).isEqualTo(result2.getPagination().getCursor());
        assertThat(result3.getPagination().getCursorPosition()).isEqualTo(result2.getPagination().getCursorPosition());
        assertThat(result3.getPagination().getHasMore()).isFalse();
        assertThat(result3.getPagination().getCurrentPage().getPageNumber())
                .isEqualTo(result3.getPagination().getTotalPages());
    }

    @Test(expected = StatusRuntimeException.class)
    public void testJobQueryWithBadCursor() {
        client.findJobs(JobQuery.newBuilder().setPage(Page.newBuilder().setPageSize(4).setCursor("bad_cursor_value")).build());
    }

    @Test
    public void testJobQueryWithCursorAndEmptyResult() {
        JobQueryResult result = client.findJobs(JobQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4))
                .putFilteringCriteria("jobState", "KillInitiated") // Filter by something that gives us empty result
                .build()
        );
        assertThat(result.getItemsList()).isEmpty();
        assertThat(result.getPagination().getCursor()).isEmpty();
        assertThat(result.getPagination().getCursorPosition()).isZero();
        assertThat(result.getPagination().getHasMore()).isFalse();
    }

    @Test
    public void testTaskQueryWithCursor() {
        // Page 0
        TaskQueryResult result0 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4)).build()
        );
        assertThat(result0.getItemsList()).containsExactlyElementsOf(allTasksInOrder.subList(0, 4));
        assertThat(result0.getPagination().getCursor()).isNotEmpty();
        assertThat(result0.getPagination().getCursorPosition()).isEqualTo(3);
        assertThat(result0.getPagination().getHasMore()).isTrue();
        assertThat(result0.getPagination().getCurrentPage().getPageNumber()).isEqualTo(0);

        // Page 1
        TaskQueryResult result1 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4).setCursor(result0.getPagination().getCursor())).build()
        );
        assertThat(result1.getItemsList()).containsExactlyElementsOf(allTasksInOrder.subList(4, 8));
        assertThat(result1.getPagination().getCursor()).isNotEmpty();
        assertThat(result1.getPagination().getCursorPosition()).isEqualTo(7);
        assertThat(result1.getPagination().getHasMore()).isTrue();
        assertThat(result1.getPagination().getCurrentPage().getPageNumber()).isEqualTo(1);

        // Page 2
        TaskQueryResult result2 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4).setCursor(result1.getPagination().getCursor())).build()
        );
        assertThat(result2.getItemsList()).containsExactlyElementsOf(allTasksInOrder.subList(8, 12));
        assertThat(result2.getPagination().getHasMore()).isFalse();
        assertThat(result2.getPagination().getCursor()).isNotEmpty();
        assertThat(result2.getPagination().getCursorPosition()).isEqualTo(11);
        assertThat(result2.getPagination().getCurrentPage().getPageNumber()).isEqualTo(2);

        // Check cursor points to the latest returned element
        TaskQueryResult result3 = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4).setCursor(result2.getPagination().getCursor())).build()
        );
        assertThat(result3.getItemsList()).isEmpty();
        assertThat(result3.getPagination().getCursor()).isEqualTo(result2.getPagination().getCursor());
        assertThat(result3.getPagination().getCursorPosition()).isEqualTo(result2.getPagination().getCursorPosition());
        assertThat(result3.getPagination().getHasMore()).isFalse();
        assertThat(result3.getPagination().getCurrentPage().getPageNumber())
                .isEqualTo(result3.getPagination().getTotalPages());
    }

    @Test(expected = StatusRuntimeException.class)
    public void testTaskQueryWithBadCursor() {
        client.findTasks(TaskQuery.newBuilder().setPage(Page.newBuilder().setPageSize(4).setCursor("bad_cursor_value")).build());
    }

    @Test
    public void testTaskQueryWithCursorAndEmptyResult() {
        TaskQueryResult result = client.findTasks(TaskQuery.newBuilder()
                .setPage(Page.newBuilder().setPageSize(4))
                .putFilteringCriteria("jobState", "KillInitiated") // Filter by something that gives us empty result
                .build()
        );
        assertThat(result.getItemsList()).isEmpty();
        assertThat(result.getPagination().getCursor()).isEmpty();
        assertThat(result.getPagination().getCursorPosition()).isZero();
        assertThat(result.getPagination().getHasMore()).isFalse();
    }
}
