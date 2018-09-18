package com.netflix.titus.master.integration.v3.job;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobCursorArchiveQueryTest {

    private static final String ALL_TASK_STATES = StringExt.concatenate(TaskStatus.TaskState.class, ",", s -> s != TaskStatus.TaskState.UNRECOGNIZED);

    private static final Page TWO_ITEM_PAGE = Page.newBuilder().setPageSize(2).build();

    private static final int TASKS_PER_JOB = 4;

    private static final TitusStackResource titusStackResource = new TitusStackResource(basicCell(5));

    private static final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private static final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private static JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    private static JobScenarioBuilder jobScenarioBuilder;

    @BeforeClass
    public static void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();

        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor()
                .but(jd -> jd.getExtensions().toBuilder()
                        .withCapacity(Capacity.newBuilder().withMin(0).withDesired(TASKS_PER_JOB).withMax(TASKS_PER_JOB).build())
                        .build()
                );

        jobScenarioBuilder = jobsScenarioBuilder.schedule(jobDescriptor, ScenarioTemplates.startTasksInNewJob()).takeJob(0);
    }

    @Test
    public void testArchiveQuery() {
        Evaluators.times(TASKS_PER_JOB, idx -> jobScenarioBuilder.getTaskByIndex(idx).killTask());
        Evaluators.times(TASKS_PER_JOB, idx -> jobScenarioBuilder.getTaskByIndex(idx).expectStateUpdateSkipOther(TaskStatus.TaskState.Finished));
        Evaluators.times(TASKS_PER_JOB, idx -> jobScenarioBuilder.expectTaskInSlot(idx, 1));

        int allTasksCount = TASKS_PER_JOB * 2;
        Set<String> taskIds = new HashSet<>();

        Page current = TWO_ITEM_PAGE;
        for (int i = 0; i < TASKS_PER_JOB; i++) {
            TaskQuery query = TaskQuery.newBuilder()
                    .setPage(current)
                    .putFilteringCriteria("jobIds", jobScenarioBuilder.getJobId())
                    .putFilteringCriteria("taskStates", ALL_TASK_STATES)
                    .build();
            TaskQueryResult result = client.findTasks(query);

            assertThat(result.getPagination().getTotalItems()).isEqualTo(allTasksCount);
            assertThat(result.getPagination().getCursor()).isNotEmpty();

            taskIds.addAll(result.getItemsList().stream().map(Task::getId).collect(Collectors.toList()));
            current = query.getPage().toBuilder().setCursor(result.getPagination().getCursor()).build();
        }

        assertThat(taskIds).hasSize(allTasksCount);
    }
}
