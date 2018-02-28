package io.netflix.titus.master.integration.v3.job;

import java.util.List;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.basicSetupActivation;
import static io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks.basicStack;
import static org.assertj.core.api.Assertions.assertThat;

public class JobCursoryQueryWithUpdatesTest {
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
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(basicSetupActivation());
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
