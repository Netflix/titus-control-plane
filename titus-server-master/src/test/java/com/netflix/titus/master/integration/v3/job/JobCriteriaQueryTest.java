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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Triple;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.master.integration.v3.job.CellAssertions.assertCellInfo;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TODO Error codes
 */
@Category(IntegrationTest.class)
public class JobCriteriaQueryTest extends BaseIntegrationTest {

    private static final Page PAGE = Page.newBuilder().setPageNumber(0).setPageSize(100).build();

    private static final String V3_ENGINE_APP = TitusStackResource.V3_ENGINE_APP_PREFIX + 1;
    private static final String V3_ENGINE_APP2 = TitusStackResource.V3_ENGINE_APP_PREFIX + 2;

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(5));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    @Test(timeout = 30_000)
    public void testFindJobAndTaskByJobIdsV3() throws Exception {
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), 3, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));

        String job0 = jobsScenarioBuilder.takeJob(0).getJobId();
        String job2 = jobsScenarioBuilder.takeJob(2).getJobId();

        // Jobs
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobIds", job0 + ',' + job2).setPage(PAGE).build());
        final List<Job> itemsList = jobQueryResult.getItemsList();
        assertThat(itemsList).hasSize(2);

        // Tasks
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobIds", job0 + ',' + job2).setPage(PAGE).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(2);
    }

    @Test(timeout = 30_000)
    public void testFindJobAndTaskByTaskIdsV3() throws Exception {
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), 3, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));

        String task0 = jobsScenarioBuilder.takeJob(0).getTaskByIndex(0).getTask().getId();
        String task2 = jobsScenarioBuilder.takeJob(2).getTaskByIndex(0).getTask().getId();

        // Jobs
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("taskIds", task0 + ',' + task2).setPage(PAGE).build());
        final List<Job> itemsList = jobQueryResult.getItemsList();
        assertThat(itemsList).hasSize(2);

        // Tasks
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("taskIds", task0 + ',' + task2).setPage(PAGE).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(2);
    }

    @Test(timeout = 60_000)
    public void testFindArchivedTasksByTaskIdsV3() throws Exception {
        int numberOfTasks = 5;
        JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor()
                .but(jd -> jd.getExtensions().toBuilder().withSize(numberOfTasks).build());

        jobsScenarioBuilder.schedule(jobDescriptor, 1, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(ScenarioTemplates.completeTask())
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.Finished, "Expected job to complete")
                .findTasks(TaskQuery.newBuilder()
                                .putFilteringCriteria("jobIds", jobsScenarioBuilder.takeJobId(0))
                                .putFilteringCriteria("taskStates", TaskStatus.TaskState.Finished.name())
                                .setPage(PAGE)
                                .build(),
                        tasks -> tasks.size() == numberOfTasks && tasks.stream().allMatch(task -> task.getStatus().getState() == TaskStatus.TaskState.Finished)));
    }

    @Test(timeout = 30_000)
    public void testSearchByJobTypeV3() throws Exception {
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), 1, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));
        jobsScenarioBuilder.schedule(oneTaskServiceJobDescriptor(), 1, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));

        String batchJobId = jobsScenarioBuilder.takeJob(0).getJobId();
        String batchTaskId = jobsScenarioBuilder.takeJob(0).getTaskByIndex(0).getTask().getId();
        String serviceJobId = jobsScenarioBuilder.takeJob(1).getJobId();
        String serviceTaskId = jobsScenarioBuilder.takeJob(1).getTaskByIndex(0).getTask().getId();

        // Batch only (jobs)
        JobQueryResult batchQueryJobs = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobType", "batch").setPage(PAGE).build());
        assertThat(batchQueryJobs.getItemsList()).hasSize(1);
        Job batchQueryJobsItem = batchQueryJobs.getItems(0);
        assertThat(batchQueryJobsItem.getId()).isEqualTo(batchJobId);

        // Batch only (tasks)
        TaskQueryResult batchQueryTasks = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobType", "batch").setPage(PAGE).build());
        assertThat(batchQueryTasks.getItemsList()).hasSize(1);
        assertThat(batchQueryTasks.getItems(0).getId()).isEqualTo(batchTaskId);

        // Service only (jobs)
        JobQueryResult serviceQueryJobs = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobType", "service").setPage(PAGE).build());
        assertThat(serviceQueryJobs.getItemsList()).hasSize(1);
        Job serviceQueryJobsItem = serviceQueryJobs.getItems(0);
        assertThat(serviceQueryJobsItem.getId()).isEqualTo(serviceJobId);

        // Service only (tasks)
        TaskQueryResult serviceQueryTasks = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobType", "service").setPage(PAGE).build());
        assertThat(serviceQueryTasks.getItemsList()).hasSize(1);
        assertThat(serviceQueryTasks.getItems(0).getId()).isEqualTo(serviceTaskId);
    }

    /**
     * V3 only.
     * <p>
     * V2 jobState query not supported, as effectively there is one state 'Accepted'. Once job moves to 'Finished' state
     * it is removed from TitusMaster.
     */
    @Test(timeout = 30_000)
    public void testSearchByJobState() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = batchJobDescriptors().getValue().toBuilder().withApplicationName(V3_ENGINE_APP).build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(taskScenarioBuilder -> {
                    taskScenarioBuilder.getTaskExecutionHolder().delayStateTransition(taskState -> Long.MAX_VALUE);
                    return taskScenarioBuilder;
                })
                .killJob()
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.KillInitiated, "Expected state: " + JobState.KillInitiated)
        );

        String acceptedJobId = jobsScenarioBuilder.takeJob(0).getJobId();
        String acceptedTaskId = jobsScenarioBuilder.takeJob(0).getTaskByIndex(0).getTask().getId();
        String killInitiatedJobId = jobsScenarioBuilder.takeJob(1).getJobId();
        String killInitiatedTaskId = jobsScenarioBuilder.takeJob(1).getTaskByIndex(0).getTask().getId();

        // Indexes are recomputed after events are sent, so if we run findJobs/findTasks immediately, they may use stale index.
        Thread.sleep(10);

        // Jobs (Accepted)
        JobQueryResult acceptedJobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobState", "Accepted").setPage(PAGE).build());
        assertThat(acceptedJobQueryResult.getItemsList()).hasSize(1);
        Job acceptedJobQueryResultItem = acceptedJobQueryResult.getItems(0);
        assertThat(acceptedJobQueryResultItem.getId()).isEqualTo(acceptedJobId);

        // Jobs (KillInitiated)
        JobQueryResult killInitJobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobState", "KillInitiated").setPage(PAGE).build());
        assertThat(killInitJobQueryResult.getItemsList()).hasSize(1);
        Job killInitJobQueryResultItem = killInitJobQueryResult.getItems(0);
        assertThat(killInitJobQueryResultItem.getId()).isEqualTo(killInitiatedJobId);

        // Tasks (Accepted)
        TaskQueryResult acceptedTaskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobState", "Accepted").setPage(PAGE).build());
        assertThat(acceptedTaskQueryResult.getItemsList()).hasSize(1);
        assertThat(acceptedTaskQueryResult.getItems(0).getId()).isEqualTo(acceptedTaskId);

        // Tasks (KillInitiated)
        TaskQueryResult killInitTaskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobState", "KillInitiated").setPage(PAGE).build());
        assertThat(killInitTaskQueryResult.getItemsList()).hasSize(1);
        assertThat(killInitTaskQueryResult.getItems(0).getId()).isEqualTo(killInitiatedTaskId);
    }

    @Test(timeout = 30_000)
    public void testSearchByTaskStateV3() throws Exception {
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startJob(TaskStatus.TaskState.Launched)));
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startJob(TaskStatus.TaskState.StartInitiated)));
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startJob(TaskStatus.TaskState.Started)));
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startJobAndMoveTasksToKillInitiated(true)));

        testSearchByTaskState("Launched", jobsScenarioBuilder.takeJobId(0), jobsScenarioBuilder.takeTaskId(0, 0));
        testSearchByTaskState("StartInitiated", jobsScenarioBuilder.takeJobId(1), jobsScenarioBuilder.takeTaskId(1, 0));
        testSearchByTaskState("Started", jobsScenarioBuilder.takeJobId(2), jobsScenarioBuilder.takeTaskId(2, 0));
        testSearchByTaskState("KillInitiated", jobsScenarioBuilder.takeJobId(3), jobsScenarioBuilder.takeTaskId(3, 0));
    }

    private void testSearchByTaskState(String taskState, String expectedJobId, String expectedTaskId) {
        // Job
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("taskStates", taskState).setPage(PAGE).build());
        assertThat(jobQueryResult.getItemsList()).hasSize(1);
        Job jobQueryResultItem = jobQueryResult.getItems(0);
        assertThat(jobQueryResultItem.getId()).isEqualTo(expectedJobId);

        // Task
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("taskStates", taskState).setPage(PAGE).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(1);
        assertThat(taskQueryResult.getItems(0).getId()).isEqualTo(expectedTaskId);
    }

    @Test(timeout = 30_000)
    public void testSearchByOwnerV3() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor1 = oneTaskBatchJobDescriptor().toBuilder().withOwner(
                JobModel.newOwner().withTeamEmail("user1@netflix.com").build()
        ).build();
        JobDescriptor<BatchJobExt> jobDescriptor2 = oneTaskBatchJobDescriptor().toBuilder().withOwner(
                JobModel.newOwner().withTeamEmail("user2@netflix.com").build()
        ).build();
        testSearchByAttributeValue(jobDescriptor1, jobDescriptor2, "owner", "user1@netflix.com", "user2@netflix.com");
    }

    @Test(timeout = 30_000)
    public void testSearchByAppNameV3() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor1 = batchJobDescriptors().getValue().toBuilder().withApplicationName(V3_ENGINE_APP).build();
        JobDescriptor<BatchJobExt> jobDescriptor2 = batchJobDescriptors().getValue().toBuilder().withApplicationName(V3_ENGINE_APP2).build();
        testSearchByAttributeValue(jobDescriptor1, jobDescriptor2, "appName", V3_ENGINE_APP, V3_ENGINE_APP2);
    }

    @Test(timeout = 30_000)
    public void testSearchByApplicationNameV3() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor1 = batchJobDescriptors().getValue().toBuilder().withApplicationName(V3_ENGINE_APP).build();
        JobDescriptor<BatchJobExt> jobDescriptor2 = batchJobDescriptors().getValue().toBuilder().withApplicationName(V3_ENGINE_APP2).build();
        testSearchByAttributeValue(jobDescriptor1, jobDescriptor2, "applicationName", V3_ENGINE_APP, V3_ENGINE_APP2);
    }

    @Test(timeout = 30_000)
    public void testSearchByCapacityGroupV3() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor1 = oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("capacity1").build();
        JobDescriptor<BatchJobExt> jobDescriptor2 = oneTaskBatchJobDescriptor().toBuilder().withCapacityGroup("capacity2").build();
        testSearchByAttributeValue(jobDescriptor1, jobDescriptor2, "capacityGroup", "capacity1", "capacity2");
    }

    @Test(timeout = 30_000)
    public void testSearchByJobGroupInfoV3() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor1 = oneTaskBatchJobDescriptor().toBuilder()
                .withJobGroupInfo(JobModel.newJobGroupInfo()
                        .withStack("stack1")
                        .withDetail("detail1")
                        .withSequence("001")
                        .build())
                .build();
        JobDescriptor<BatchJobExt> jobDescriptor2 = oneTaskBatchJobDescriptor().toBuilder()
                .withJobGroupInfo(JobModel.newJobGroupInfo()
                        .withStack("stack2")
                        .withDetail("detail2")
                        .withSequence("002")
                        .build())
                .build();
        testSearchByAttributeValue(
                jobDescriptor1,
                jobDescriptor2,
                Triple.of("jobGroupStack", "stack1", "stack2"),
                Triple.of("jobGroupDetail", "detail1", "detail2"),
                Triple.of("jobGroupSequence", "001", "002")
        );
    }

    @Test(timeout = 30_000)
    public void testSearchByImageV3() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor1 = oneTaskBatchJobDescriptor().but(j -> j.getContainer().toBuilder().withImage(
                JobModel.newImage().withName("image1").withTag("tag1").build()
        ));
        JobDescriptor<BatchJobExt> jobDescriptor2 = oneTaskBatchJobDescriptor().but(j -> j.getContainer().toBuilder().withImage(
                JobModel.newImage().withName("image2").withTag("tag2").build()
        ));
        testSearchByAttributeValue(jobDescriptor1, jobDescriptor2,
                Triple.of("imageName", "image1", "image2"),
                Triple.of("imageTag", "tag1", "tag2")
        );
    }

    @Test(timeout = 30_000)
    public void testSearchByJobDescriptorAttributesV3() throws Exception {
        for (int i = 0; i < 3; i++) {
            JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor().toBuilder()
                    .withAttributes(CollectionsExt.asMap(
                            String.format("job%d.key1", i), "value1",
                            String.format("job%d.key2", i), "value2"
                    ))
                    .build();
            jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));
        }

        String job0 = jobsScenarioBuilder.takeJob(0).getJobId();
        String task0 = jobsScenarioBuilder.takeJob(0).getTaskByIndex(0).getTask().getId();
        String job1 = jobsScenarioBuilder.takeJob(1).getJobId();
        String task1 = jobsScenarioBuilder.takeJob(1).getTaskByIndex(0).getTask().getId();

        // Jobs
        assertContainsJobs(
                client.findJobs(JobQuery.newBuilder()
                        .putFilteringCriteria("attributes", "job0.key1,job1.key1")
                        .putFilteringCriteria("attributes.op", "or")
                        .setPage(PAGE).build()
                ),
                job0, job1
        );
        assertContainsJobs(
                client.findJobs(JobQuery.newBuilder()
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key1:value2")
                        .putFilteringCriteria("attributes.op", "or")
                        .setPage(PAGE).build()
                ),
                job0
        );
        assertContainsJobs(
                client.findJobs(JobQuery.newBuilder()
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key2:value2")
                        .putFilteringCriteria("attributes.op", "and")
                        .setPage(PAGE).build()
                ),
                job0
        );

        // Tasks
        assertContainsTasks(
                client.findTasks(TaskQuery.newBuilder()
                        .putFilteringCriteria("attributes", "job0.key1,job1.key1")
                        .putFilteringCriteria("attributes.op", "or")
                        .setPage(PAGE).build()
                ),
                task0, task1
        );
        assertContainsTasks(
                client.findTasks(TaskQuery.newBuilder()
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key1:value2")
                        .putFilteringCriteria("attributes.op", "or")
                        .setPage(PAGE).build()
                ),
                task0
        );
        assertContainsTasks(
                client.findTasks(TaskQuery.newBuilder()
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key2:value2")
                        .putFilteringCriteria("attributes.op", "and")
                        .setPage(PAGE).build()
                ),
                task0
        );
    }

    @Test(timeout = 30_000)
    public void testSearchByCellV3() throws Exception {
        final int numberOfJobs = 3;
        String[] expectedJobIds = new String[numberOfJobs];
        String[] expectedTaskIds = new String[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));
            expectedJobIds[i] = jobsScenarioBuilder.takeJob(i).getJobId();
            expectedTaskIds[i] = jobsScenarioBuilder.takeJob(i).getTaskByIndex(0).getTask().getId();
        }

        // Jobs
        JobQueryResult jobs1 = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("attributes", "titus.cell,titus.stack")
                .putFilteringCriteria("attributes.op", "or")
                .setPage(PAGE)
                .build()
        );
        assertContainsJobs(jobs1, expectedJobIds);
        jobs1.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        JobQueryResult jobs2 = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("attributes", "titus.cell")
                .putFilteringCriteria("attributes.op", "and")
                .setPage(PAGE)
                .build()
        );
        assertContainsJobs(jobs2, expectedJobIds);
        jobs2.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        JobQueryResult jobs3 = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s,titus.stack:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "or")
                .setPage(PAGE)
                .build()
        );
        assertContainsJobs(jobs3, expectedJobIds);
        jobs3.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        JobQueryResult jobs4 = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "and")
                .setPage(PAGE)
                .build()
        );
        assertContainsJobs(jobs4, expectedJobIds);
        jobs4.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        // Tasks
        TaskQueryResult tasks1 = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("attributes", "titus.cell,titus.stack")
                .putFilteringCriteria("attributes.op", "or")
                .setPage(PAGE)
                .build()
        );
        assertContainsTasks(tasks1, expectedTaskIds);
        tasks1.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));

        TaskQueryResult tasks2 = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("attributes", "titus.cell")
                .putFilteringCriteria("attributes.op", "and")
                .setPage(PAGE)
                .build()
        );
        assertContainsTasks(tasks2, expectedTaskIds);
        tasks2.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));

        TaskQueryResult tasks3 = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s,titus.stack:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "or")
                .setPage(PAGE).build()
        );
        assertContainsTasks(tasks3, expectedTaskIds);
        tasks3.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));

        final TaskQueryResult tasks4 = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "and")
                .setPage(PAGE).build()
        );
        assertContainsTasks(tasks4, expectedTaskIds);
        tasks4.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));
    }

    private void testSearchByAttributeValue(JobDescriptor<BatchJobExt> jobDescriptor1,
                                            JobDescriptor<BatchJobExt> jobDescriptor2,
                                            String attributeName,
                                            String job1Value,
                                            String job2Value) throws Exception {
        testSearchByAttributeValue(jobDescriptor1, jobDescriptor2, Triple.of(attributeName, job1Value, job2Value));
    }

    private void testSearchByAttributeValue(JobDescriptor<BatchJobExt> jobDescriptor1,
                                            JobDescriptor<BatchJobExt> jobDescriptor2,
                                            Triple<String, String, String>... attributeValue1Value2Triples) throws Exception {
        jobsScenarioBuilder.schedule(jobDescriptor1, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));
        jobsScenarioBuilder.schedule(jobDescriptor2, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob()));

        String job0 = jobsScenarioBuilder.takeJob(0).getJobId();
        String task0 = jobsScenarioBuilder.takeJob(0).getTaskByIndex(0).getTask().getId();
        String job1 = jobsScenarioBuilder.takeJob(1).getJobId();
        String task1 = jobsScenarioBuilder.takeJob(1).getTaskByIndex(0).getTask().getId();

        for (Triple<String, String, String> next : attributeValue1Value2Triples) {
            String attributeName = next.getFirst();
            String job1Value = next.getSecond();
            String job2Value = next.getThird();

            // Jobs
            JobQueryResult jobQueryResult1 = client.findJobs(JobQuery.newBuilder().putFilteringCriteria(attributeName, job1Value).setPage(PAGE).build());
            assertThat(jobQueryResult1.getItemsList()).hasSize(1);
            final Job jobQueryResult1Item = jobQueryResult1.getItems(0);
            assertThat(jobQueryResult1Item.getId()).isEqualTo(job0);

            JobQueryResult jobQueryResult2 = client.findJobs(JobQuery.newBuilder().putFilteringCriteria(attributeName, job2Value).setPage(PAGE).build());
            assertThat(jobQueryResult2.getItemsList()).hasSize(1);
            final Job jobQueryResult2Item = jobQueryResult2.getItems(0);
            assertThat(jobQueryResult2Item.getId()).isEqualTo(job1);

            // Tasks
            TaskQueryResult taskQueryResult1 = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria(attributeName, job1Value).setPage(PAGE).build());
            assertThat(taskQueryResult1.getItemsList()).hasSize(1);
            assertThat(taskQueryResult1.getItems(0).getId()).isEqualTo(task0);

            TaskQueryResult taskQueryResult2 = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria(attributeName, job2Value).setPage(PAGE).build());
            assertThat(taskQueryResult2.getItemsList()).hasSize(1);
            assertThat(taskQueryResult2.getItems(0).getId()).isEqualTo(task1);
        }
    }

    @Test(timeout = 30_000)
    public void testPagination() throws Exception {
        // Create a mix of batch and service jobs.
        jobsScenarioBuilder.schedule(oneTaskBatchJobDescriptor(), 3, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startJob(TaskStatus.TaskState.Started)));
        jobsScenarioBuilder.schedule(oneTaskServiceJobDescriptor(), 3, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startJob(TaskStatus.TaskState.Started)));

        Page firstPageOf5 = Page.newBuilder().setPageNumber(0).setPageSize(5).build();
        Page secondPageOf5 = Page.newBuilder().setPageNumber(1).setPageSize(5).build();

        // Jobs
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder().setPage(firstPageOf5).build());
        assertThat(jobQueryResult.getItemsList()).hasSize(5);
        checkPage(jobQueryResult.getPagination(), firstPageOf5, 2, 6, true);

        JobQueryResult jobQueryResult2 = client.findJobs(JobQuery.newBuilder().setPage(secondPageOf5).build());
        assertThat(jobQueryResult2.getItemsList()).hasSize(1);
        checkPage(jobQueryResult2.getPagination(), secondPageOf5, 2, 6, false);

        Set<String> foundJobIds = new HashSet<>();
        jobQueryResult.getItemsList().forEach(j -> foundJobIds.add(j.getId()));
        jobQueryResult2.getItemsList().forEach(j -> foundJobIds.add(j.getId()));
        assertThat(foundJobIds).hasSize(6);

        // Tasks
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder().setPage(firstPageOf5).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(5);
        checkPage(taskQueryResult.getPagination(), firstPageOf5, 2, 6, true);

        TaskQueryResult taskQueryResult2 = client.findTasks(TaskQuery.newBuilder().setPage(secondPageOf5).build());
        assertThat(taskQueryResult2.getItemsList()).hasSize(1);
        checkPage(taskQueryResult2.getPagination(), secondPageOf5, 2, 6, false);

        Set<String> foundTasksIds = new HashSet<>();
        taskQueryResult.getItemsList().forEach(j -> foundTasksIds.add(j.getId()));
        taskQueryResult2.getItemsList().forEach(j -> foundTasksIds.add(j.getId()));
        assertThat(foundTasksIds).hasSize(6);
    }

    private void checkPage(Pagination pagination, Page current, int totalPages, int totalItems, boolean hasMore) {
        assertThat(pagination.getCurrentPage()).isEqualTo(current);
        assertThat(pagination.getTotalPages()).isEqualTo(totalPages);
        assertThat(pagination.getTotalItems()).isEqualTo(totalItems);
        assertThat(pagination.getHasMore()).isEqualTo(hasMore);
    }

    @Test(timeout = 30_000)
    public void testFieldsFiltering() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor().toBuilder()
                .withAttributes(ImmutableMap.of("keyA", "valueA", "keyB", "valueB"))
                .build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.startTasks()));

        // Check jobs
        List<Job> foundJobs = client.findJobs(JobQuery.newBuilder().setPage(PAGE)
                .addFields("status")
                .addFields("jobDescriptor.attributes.keyA")
                .build()
        ).getItemsList();
        assertThat(foundJobs).hasSize(1);
        assertThat(foundJobs.get(0).getId()).isNotEmpty(); // Always present
        assertThat(foundJobs.get(0).getStatus().getReasonMessage()).isNotEmpty();
        final com.netflix.titus.grpc.protogen.JobDescriptor foundJobDescriptor = foundJobs.get(0).getJobDescriptor();
        assertThat(foundJobDescriptor.getAttributesMap()).isNotEmpty();
        assertThat(foundJobDescriptor.getAttributesMap()).containsEntry("keyA", "valueA");

        // Check tasks
        List<Task> foundTasks = client.findTasks(TaskQuery.newBuilder().setPage(PAGE)
                .addFields("status")
                .addFields("statusHistory")
                .build()
        ).getItemsList();
        assertThat(foundTasks).hasSize(1);
        assertThat(foundTasks.get(0).getId()).isNotEmpty(); // Always present
        assertThat(foundTasks.get(0).getStatus().getReasonMessage()).isNotEmpty();
        assertThat(foundTasks.get(0).getStatusHistoryList()).isNotEmpty();
        assertThat(foundTasks.get(0).getTaskContextMap()).isEmpty();
    }

    private void assertContainsJobs(JobQueryResult queryResult, String... jobIds) {
        assertThat(queryResult.getItemsCount()).isEqualTo(jobIds.length);
        Set<String> returnedJobIds = queryResult.getItemsList().stream().map(Job::getId).collect(Collectors.toSet());
        assertThat(returnedJobIds).contains(jobIds);
    }

    private void assertContainsTasks(TaskQueryResult queryResult, String... taskIds) {
        assertThat(queryResult.getItemsCount()).isEqualTo(taskIds.length);
        Set<String> returnedTaskIds = queryResult.getItemsList().stream().map(com.netflix.titus.grpc.protogen.Task::getId).collect(Collectors.toSet());
        assertThat(returnedTaskIds).contains(taskIds);
    }
}
