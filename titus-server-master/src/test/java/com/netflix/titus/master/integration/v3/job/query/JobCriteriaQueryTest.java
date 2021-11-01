/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.job.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Owner;
import com.netflix.titus.api.jobmanager.model.job.PlatformSidecar;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.CollectionsExt;
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
import com.netflix.titus.master.integration.v3.scenario.JobScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.master.integration.v3.job.CellAssertions.assertCellInfo;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * As queries are immutable, the tests share single stack which is preloaded with all the data required for all test cases.
 * Furthermore each test may add its own data set.
 */
@Category(IntegrationTest.class)
public class JobCriteriaQueryTest extends BaseIntegrationTest {

    private static final Page PAGE = Page.newBuilder().setPageNumber(0).setPageSize(100).build();

    private final static TitusStackResource titusStackResource = new TitusStackResource(
            EmbeddedTitusCell.aTitusCell()
                    .withMaster(EmbeddedTitusMasters.basicMasterWithKubeIntegration(EmbeddedKubeClusters.basicClusterWithLargeInstances(20)).toBuilder()
                            .withCellName("embeddedCell")
                            // Set to very high value as we do not want to expire it.
                            .withProperty("titusMaster.jobManager.taskInLaunchedStateTimeoutMs", "30000000")
                            .withProperty("titusMaster.jobManager.batchTaskInStartInitiatedStateTimeoutMs", "30000000")
                            .withProperty("titusMaster.jobManager.serviceTaskInStartInitiatedStateTimeoutMs", "30000000")
                            .build()
                    )
                    .withDefaultGateway()
                    .build()
    );

    private final static JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(jobsScenarioBuilder);

    private static final String BATCH_OWNER = "batchOwner@netflix.com";
    private static final String BATCH_APPLICATION = "batchApplication";
    private static final String BATCH_CAPACITY_GROUP = "batchCapacityGroup";
    private static final JobGroupInfo BATCH_JOB_GROUP_INFO = JobGroupInfo.newBuilder().withStack("batchStack").withDetail("batchDetail").withSequence("batch001").build();
    private static final String BATCH_IMAGE_NAME = "batchImageName";
    private static final String BATCH_IMAGE_TAG = "batchImageTag";

    private static final String SERVICE_OWNER = "serviceOwner@netflix.com";
    private static final String SERVICE_APPLICATION = "serviceApplication";
    private static final String SERVICE_CAPACITY_GROUP = "serviceCapacityGroup";
    private static final JobGroupInfo SERVICE_JOB_GROUP_INFO = JobGroupInfo.newBuilder().withStack("serviceStack").withDetail("serviceDetail").withSequence("service001").build();
    private static final String SERVICE_IMAGE_NAME = "serviceImageName";
    private static final String SERVICE_IMAGE_TAG = "serviceImageTag";

    /**
     * Add to jobs created in the setup method.
     */
    private static final String PRE_CREATED_JOBS_LABEL = "precreatedJob";

    private static final JobDescriptor<BatchJobExt> BATCH_JOB_TEMPLATE = oneTaskBatchJobDescriptor();
    private static final JobDescriptor<ServiceJobExt> SERVICE_JOB_TEMPLATE = oneTaskServiceJobDescriptor();

    private static JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    private static final List<String> batchJobsWithCreatedTasks = new ArrayList<>();
    private static final List<String> batchTasks = new ArrayList<>();
    private static final List<String> serviceJobsWithCreatedTasks = new ArrayList<>();
    private static final List<String> serviceTasks = new ArrayList<>();

    private static String finishedBatchJobWithFiveTasks;

    @BeforeClass
    public static void setUp() throws Exception {
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();

        // Jobs with launched tasks
        JobDescriptor<BatchJobExt> batchJobDescriptor = BATCH_JOB_TEMPLATE.toBuilder()
                .withOwner(Owner.newBuilder().withTeamEmail(BATCH_OWNER).build())
                .withApplicationName(BATCH_APPLICATION)
                .withCapacityGroup(BATCH_CAPACITY_GROUP)
                .withJobGroupInfo(BATCH_JOB_GROUP_INFO)
                .withContainer(BATCH_JOB_TEMPLATE.getContainer().toBuilder()
                        .withImage(Image.newBuilder().withName(BATCH_IMAGE_NAME).withTag(BATCH_IMAGE_TAG).build())
                        .build()
                )
                .withAttributes(Collections.singletonMap(PRE_CREATED_JOBS_LABEL, "true"))
                .build();
        jobsScenarioBuilder.schedule(batchJobDescriptor, 3,
                jobScenarioBuilder -> jobScenarioBuilder
                        .template(ScenarioTemplates.launchJob())
                        .inJob(job -> batchJobsWithCreatedTasks.add(job.getId()))
        );
        batchJobsWithCreatedTasks.forEach(jobId -> {
            String taskId = jobsScenarioBuilder.takeJob(jobId).getTaskByIndex(0).getTask().getId();
            batchTasks.add(taskId);
        });
        JobDescriptor<ServiceJobExt> serviceJobDescriptor = SERVICE_JOB_TEMPLATE.toBuilder()
                .withOwner(Owner.newBuilder().withTeamEmail(SERVICE_OWNER).build())
                .withApplicationName(SERVICE_APPLICATION)
                .withCapacityGroup(SERVICE_CAPACITY_GROUP)
                .withJobGroupInfo(SERVICE_JOB_GROUP_INFO)
                .withContainer(SERVICE_JOB_TEMPLATE.getContainer().toBuilder()
                        .withImage(Image.newBuilder().withName(SERVICE_IMAGE_NAME).withTag(SERVICE_IMAGE_TAG).build())
                        .build()
                )
                .withAttributes(Collections.singletonMap(PRE_CREATED_JOBS_LABEL, "true"))
                .build();
        jobsScenarioBuilder.schedule(serviceJobDescriptor, 3,
                jobScenarioBuilder -> jobScenarioBuilder
                        .template(ScenarioTemplates.launchJob())
                        .inJob(job -> serviceJobsWithCreatedTasks.add(job.getId()))
        );
        serviceJobsWithCreatedTasks.forEach(jobId -> {
            String taskId = jobsScenarioBuilder.takeJob(jobId).getTaskByIndex(0).getTask().getId();
            serviceTasks.add(taskId);
        });

        // Finished job with 5 tasks
        int numberOfTasks = 5;
        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_TEMPLATE
                .but(jd -> jd.getExtensions().toBuilder().withSize(numberOfTasks).build());

        jobsScenarioBuilder.schedule(jobDescriptor, 1, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .allTasks(ScenarioTemplates.completeTask())
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.Finished, "Expected job to complete")
                .inJob(job -> finishedBatchJobWithFiveTasks = job.getId())
        );
    }

    @Test(timeout = 30_000)
    public void testFindJobAndTaskByJobIdsV3() {
        String job0 = batchJobsWithCreatedTasks.get(0);
        String job2 = batchJobsWithCreatedTasks.get(2);

        // Jobs
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobIds", job0 + ',' + job2).setPage(PAGE).build());
        final List<Job> itemsList = jobQueryResult.getItemsList();
        assertThat(itemsList).hasSize(2);

        // Tasks
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobIds", job0 + ',' + job2).setPage(PAGE).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(2);
    }

    @Test(timeout = 30_000)
    public void testFindJobAndTaskByTaskIdsV3() {
        String task0 = jobsScenarioBuilder.takeJob(batchJobsWithCreatedTasks.get(0)).getTaskByIndex(0).getTask().getId();
        String task2 = jobsScenarioBuilder.takeJob(batchJobsWithCreatedTasks.get(2)).getTaskByIndex(0).getTask().getId();

        // Jobs
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("taskIds", task0 + ',' + task2).setPage(PAGE).build());
        final List<Job> itemsList = jobQueryResult.getItemsList();
        assertThat(itemsList).hasSize(2);

        // Tasks
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("taskIds", task0 + ',' + task2).setPage(PAGE).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(2);
    }

    @Test(timeout = 60_000)
    public void testFindArchivedTasksByTaskIdsV3() {
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("jobIds", finishedBatchJobWithFiveTasks)
                .putFilteringCriteria("taskStates", com.netflix.titus.grpc.protogen.TaskStatus.TaskState.Finished.name())
                .setPage(PAGE)
                .build()
        );

        List<Task> tasks = taskQueryResult.getItemsList();
        assertThat(tasks).hasSize(5);
        assertThat(tasks).allMatch(task -> task.getStatus().getState() == TaskStatus.TaskState.Finished);
    }

    @Test(timeout = 30_000)
    public void testSearchByJobTypeV3() {
        // Batch only (jobs)
        JobQueryResult batchQueryJobs = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobType", "batch").setPage(PAGE).build());
        Set<String> batchJobIds = batchQueryJobs.getItemsList().stream().map(Job::getId).collect(Collectors.toSet());
        assertThat(batchJobIds).containsAll(batchJobsWithCreatedTasks);

        // Batch only (tasks)
        TaskQueryResult batchQueryTasks = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobType", "batch").setPage(PAGE).build());
        Set<String> batchTaskIds = batchQueryTasks.getItemsList().stream().map(Task::getId).collect(Collectors.toSet());
        assertThat(batchTaskIds).containsAll(batchTasks);

        // Service only (jobs)
        JobQueryResult serviceQueryJobs = client.findJobs(JobQuery.newBuilder().putFilteringCriteria("jobType", "service").setPage(PAGE).build());
        Set<String> serviceJobIds = serviceQueryJobs.getItemsList().stream().map(Job::getId).collect(Collectors.toSet());
        assertThat(serviceJobIds).containsAll(serviceJobsWithCreatedTasks);

        // Service only (tasks)
        TaskQueryResult serviceQueryTasks = client.findTasks(TaskQuery.newBuilder().putFilteringCriteria("jobType", "service").setPage(PAGE).build());
        Set<String> serviceTaskIds = serviceQueryTasks.getItemsList().stream().map(Task::getId).collect(Collectors.toSet());
        assertThat(serviceTaskIds).containsAll(serviceTasks);
    }

    @Test(timeout = 30_000)
    public void testSearchByJobState() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = batchJobDescriptors().getValue().toBuilder().withApplicationName("testSearchByJobState").build();
        String acceptedJobId = jobsScenarioBuilder.scheduleAndReturnJob(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob())).getId();
        String killInitiatedJobId = jobsScenarioBuilder.scheduleAndReturnJob(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .killJob()
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.KillInitiated, "Expected state: " + JobState.KillInitiated)
        ).getId();

        String acceptedTaskId = jobsScenarioBuilder.takeJob(acceptedJobId).getTaskByIndex(0).getTask().getId();
        String killInitiatedTaskId = jobsScenarioBuilder.takeJob(killInitiatedJobId).getTaskByIndex(0).getTask().getId();

        // Indexes are recomputed after events are sent, so if we run findJobs/findTasks immediately, they may use stale index.
        Thread.sleep(10);

        JobQuery.Builder jobQueryBuilder = JobQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByJobState")
                .setPage(PAGE);
        TaskQuery.Builder taskQueryBuilder = TaskQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByJobState")
                .setPage(PAGE);

        // Jobs (Accepted)
        JobQueryResult acceptedJobQueryResult = client.findJobs(jobQueryBuilder.putFilteringCriteria("jobState", "Accepted").build());
        assertThat(acceptedJobQueryResult.getItemsList()).hasSize(1);
        Job acceptedJobQueryResultItem = acceptedJobQueryResult.getItems(0);
        assertThat(acceptedJobQueryResultItem.getId()).isEqualTo(acceptedJobId);

        // Jobs (KillInitiated)
        JobQueryResult killInitJobQueryResult = client.findJobs(jobQueryBuilder.putFilteringCriteria("jobState", "KillInitiated").setPage(PAGE).build());
        assertThat(killInitJobQueryResult.getItemsList()).hasSize(1);
        Job killInitJobQueryResultItem = killInitJobQueryResult.getItems(0);
        assertThat(killInitJobQueryResultItem.getId()).isEqualTo(killInitiatedJobId);

        // Tasks (Accepted)
        TaskQueryResult acceptedTaskQueryResult = client.findTasks(taskQueryBuilder.putFilteringCriteria("jobState", "Accepted").setPage(PAGE).build());
        assertThat(acceptedTaskQueryResult.getItemsList()).hasSize(1);
        assertThat(acceptedTaskQueryResult.getItems(0).getId()).isEqualTo(acceptedTaskId);

        // Tasks (KillInitiated)
        TaskQueryResult killInitTaskQueryResult = client.findTasks(taskQueryBuilder.putFilteringCriteria("jobState", "KillInitiated").setPage(PAGE).build());
        assertThat(killInitTaskQueryResult.getItemsList()).hasSize(1);
        assertThat(killInitTaskQueryResult.getItems(0).getId()).isEqualTo(killInitiatedTaskId);
    }

    @Test(timeout = 30_000)
    public void testSearchByTaskStateV3() {
        Function<Function<JobScenarioBuilder, JobScenarioBuilder>, String> jobSubmitter = template ->
                jobsScenarioBuilder.scheduleAndReturnJob(
                        BATCH_JOB_TEMPLATE.toBuilder().withApplicationName("testSearchByTaskStateV3").build(),
                        jobScenarioBuilder -> jobScenarioBuilder.template(template)
                ).getId();

        String jobLaunchedId = jobSubmitter.apply(ScenarioTemplates.launchJob());
        String startInitiatedJobId = jobSubmitter.apply(ScenarioTemplates.startJob(TaskStatus.TaskState.StartInitiated));
        String startedJobId = jobSubmitter.apply(ScenarioTemplates.startJob(TaskStatus.TaskState.Started));
        String killInitiatedJobId = jobSubmitter.apply(ScenarioTemplates.startJobAndMoveTasksToKillInitiated());

        testSearchByTaskStateV3("Launched", jobLaunchedId, jobsScenarioBuilder.takeTaskId(jobLaunchedId, 0));
        testSearchByTaskStateV3("StartInitiated", startInitiatedJobId, jobsScenarioBuilder.takeTaskId(startInitiatedJobId, 0));
        testSearchByTaskStateV3("Started", startedJobId, jobsScenarioBuilder.takeTaskId(startedJobId, 0));

        testSearchByTaskStateV3("KillInitiated", killInitiatedJobId, jobsScenarioBuilder.takeTaskId(killInitiatedJobId, 0));
    }

    private void testSearchByTaskStateV3(String taskState, String expectedJobId, String expectedTaskId) {
        // Job
        JobQueryResult jobQueryResult = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByTaskStateV3")
                .putFilteringCriteria("taskStates", taskState)
                .setPage(PAGE)
                .build()
        );
        assertThat(jobQueryResult.getItemsList()).hasSize(1);
        Job jobQueryResultItem = jobQueryResult.getItems(0);
        assertThat(jobQueryResultItem.getId()).isEqualTo(expectedJobId);

        // Task
        TaskQueryResult taskQueryResult = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByTaskStateV3")
                .putFilteringCriteria("taskStates", taskState)
                .setPage(PAGE)
                .build()
        );
        assertThat(taskQueryResult.getItemsList()).hasSize(1);
        assertThat(taskQueryResult.getItems(0).getId()).isEqualTo(expectedTaskId);
    }

    @Test(timeout = 30_000)
    public void testSearchByTaskReasonInFinishedJobV3() {
        JobDescriptor<BatchJobExt> jobDescriptor = JobFunctions.changeBatchJobSize(BATCH_JOB_TEMPLATE, 2);

        String jobId = jobsScenarioBuilder.scheduleAndReturnJob(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.launchJob())
                .inTask(0, TaskScenarioBuilder::failTaskExecution)
                .schedule()
                .inTask(1, taskScenarioBuilder -> taskScenarioBuilder
                        .transitionTo(TaskStatus.TaskState.StartInitiated)
                        .transitionTo(TaskStatus.TaskState.Started)
                        .template(ScenarioTemplates.completeTask())
                )
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.Finished, "Expected job to complete")
        ).getId();

        List<Task> task0List = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("jobIds", jobId)
                .putFilteringCriteria("taskStates", TaskStatus.TaskState.Finished.name())
                .putFilteringCriteria("taskStateReasons", "failed")
                .setPage(PAGE)
                .build()
        ).getItemsList();
        assertThat(task0List).hasSize(1);
        assertThat(task0List.get(0).getStatus().getReasonCode()).isEqualTo("failed");

        List<Task> task1List = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("jobIds", jobId)
                .putFilteringCriteria("taskStates", TaskStatus.TaskState.Finished.name())
                .putFilteringCriteria("taskStateReasons", "normal")
                .setPage(PAGE)
                .build()
        ).getItemsList();
        assertThat(task1List).hasSize(1);
        assertThat(task1List.get(0).getStatus().getReasonCode()).isEqualTo("normal");
    }

    @Test(timeout = 30_000)
    public void testSearchByOwnerV3() {
        testBatchSearchBy("owner", BATCH_OWNER);
        testServiceSearchBy("owner", SERVICE_OWNER);
    }

    @Test(timeout = 30_000)
    public void testSearchByAppNameV3() {
        testBatchSearchBy("appName", BATCH_APPLICATION);
        testServiceSearchBy("appName", SERVICE_APPLICATION);
    }

    @Test(timeout = 30_000)
    public void testSearchByApplicationNameV3() {
        testBatchSearchBy("applicationName", BATCH_APPLICATION);
        testServiceSearchBy("applicationName", SERVICE_APPLICATION);
    }

    @Test(timeout = 30_000)
    public void testSearchByCapacityGroupV3() {
        testBatchSearchBy("capacityGroup", BATCH_CAPACITY_GROUP);
        testServiceSearchBy("capacityGroup", SERVICE_CAPACITY_GROUP);
    }

    @Test(timeout = 30_000)
    public void testSearchByJobGroupInfoV3() {
        testBatchSearchBy("jobGroupStack", BATCH_JOB_GROUP_INFO.getStack());
        testBatchSearchBy("jobGroupDetail", BATCH_JOB_GROUP_INFO.getDetail());

        testServiceSearchBy("jobGroupStack", SERVICE_JOB_GROUP_INFO.getStack());
        testServiceSearchBy("jobGroupDetail", SERVICE_JOB_GROUP_INFO.getDetail());

        for (String jobId : CollectionsExt.merge(batchJobsWithCreatedTasks, serviceJobsWithCreatedTasks)) {
            testSearchByJobGroupSequence(
                    jobId,
                    jobsScenarioBuilder.takeJob(jobId).getJob().getJobDescriptor().getJobGroupInfo().getSequence()
            );
        }
    }

    @Test(timeout = 30_000)
    public void testSearchByPlatformSidecarV3() {
        List<PlatformSidecar> ps = Collections.singletonList(PlatformSidecar.newBuilder().withName("testPlatformSidecar").withChannel("").build());
        JobDescriptor<BatchJobExt> jobDescriptorWithTestSidecar = BATCH_JOB_TEMPLATE.toBuilder()
                .withApplicationName("testAppThatDoesHavePlatformSidecar")
                .withPlatformSidecars(ps)
                .build();
        jobsScenarioBuilder.schedule(jobDescriptorWithTestSidecar, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
        );

        List<Job> foundJobs = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("platformSidecar", "testPlatformSidecar")
                .setPage(PAGE)
                .build()
        ).getItemsList();
        assertThat(foundJobs).hasSize(1);
        assertThat(foundJobs.get(0).getId()).isNotEmpty(); // Always present
        com.netflix.titus.grpc.protogen.JobDescriptor foundJobDescriptor = foundJobs.get(0).getJobDescriptor();
        assertThat(foundJobDescriptor.getPlatformSidecarsList()).isNotEmpty();
        assertThat(foundJobDescriptor.getPlatformSidecarsList().get(0).getName()).isEqualTo("testPlatformSidecar");
    }

    private void testBatchSearchBy(String queryKey, String queryValue) {
        List<Job> batchJobs = client.findJobs(newJobQuery(queryKey, queryValue)).getItemsList();
        assertThat(batchJobs).hasSize(batchJobsWithCreatedTasks.size());
        assertThat(batchJobs.stream().map(Job::getId)).containsAll(batchJobsWithCreatedTasks);
    }

    private void testServiceSearchBy(String queryKey, String queryValue) {
        List<Job> serviceJobs = client.findJobs(newJobQuery(queryKey, queryValue)).getItemsList();
        assertThat(serviceJobs).hasSize(serviceJobsWithCreatedTasks.size());
        assertThat(serviceJobs.stream().map(Job::getId)).containsAll(serviceJobsWithCreatedTasks);
    }

    private void testSearchByJobGroupSequence(String expectedJobId, String sequence) {
        List<Job> jobIds = client.findJobs(newJobQuery("jobGroupSequence", sequence)).getItemsList();
        assertThat(jobIds).hasSize(1);
        assertThat(jobIds.get(0).getId()).isEqualTo(expectedJobId);
    }

    @Test(timeout = 30_000)
    public void testSearchByImageV3() {
        testBatchSearchBy("imageName", BATCH_IMAGE_NAME);
        testBatchSearchBy("imageTag", BATCH_IMAGE_TAG);
        testServiceSearchBy("imageName", SERVICE_IMAGE_NAME);
        testServiceSearchBy("imageTag", SERVICE_IMAGE_TAG);
    }

    @Test(timeout = 30_000)
    public void testSearchByJobDescriptorAttributesV3() {
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_TEMPLATE.toBuilder()
                    .withApplicationName("testSearchByJobDescriptorAttributesV3")
                    .withAttributes(CollectionsExt.asMap(
                            String.format("job%d.key1", i), "value1",
                            String.format("job%d.key2", i), "value2"
                    ))
                    .build();
            String jobId = jobsScenarioBuilder.scheduleAndReturnJob(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob())).getId();
            jobIds.add(jobId);
        }

        String job0 = jobIds.get(0);
        String task0 = jobsScenarioBuilder.takeJob(job0).getTaskByIndex(0).getTask().getId();
        String job1 = jobIds.get(1);
        String task1 = jobsScenarioBuilder.takeJob(job1).getTaskByIndex(0).getTask().getId();

        // Jobs
        JobQuery.Builder jobQueryBuilder = JobQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByJobDescriptorAttributesV3")
                .setPage(PAGE);
        assertContainsJobs(
                client.findJobs(jobQueryBuilder
                        .putFilteringCriteria("attributes", "job0.key1,job1.key1")
                        .putFilteringCriteria("attributes.op", "or")
                        .build()
                ),
                job0, job1
        );
        assertContainsJobs(
                client.findJobs(jobQueryBuilder
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key1:value2")
                        .putFilteringCriteria("attributes.op", "or")
                        .build()
                ),
                job0
        );
        assertContainsJobs(
                client.findJobs(jobQueryBuilder
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key2:value2")
                        .putFilteringCriteria("attributes.op", "and")
                        .build()
                ),
                job0
        );

        // Tasks
        TaskQuery.Builder taskQueryBuilder = TaskQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByJobDescriptorAttributesV3")
                .setPage(PAGE);
        assertContainsTasks(
                client.findTasks(taskQueryBuilder
                        .putFilteringCriteria("attributes", "job0.key1,job1.key1")
                        .putFilteringCriteria("attributes.op", "or")
                        .build()
                ),
                task0, task1
        );
        assertContainsTasks(
                client.findTasks(taskQueryBuilder
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key1:value2")
                        .putFilteringCriteria("attributes.op", "or")
                        .build()
                ),
                task0
        );
        assertContainsTasks(
                client.findTasks(taskQueryBuilder
                        .putFilteringCriteria("attributes", "job0.key1:value1,job0.key2:value2")
                        .putFilteringCriteria("attributes.op", "and")
                        .build()
                ),
                task0
        );
    }

    @Test(timeout = 30_000)
    public void testSearchByCellV3() {
        final int numberOfJobs = 3;
        String[] expectedJobIds = new String[numberOfJobs];
        String[] expectedTaskIds = new String[numberOfJobs];
        for (int i = 0; i < numberOfJobs; i++) {
            String jobId = jobsScenarioBuilder.scheduleAndReturnJob(
                    BATCH_JOB_TEMPLATE.toBuilder().withApplicationName("testSearchByCellV3").build(),
                    jobScenarioBuilder -> jobScenarioBuilder.template(ScenarioTemplates.launchJob())
            ).getId();
            expectedJobIds[i] = jobId;
            expectedTaskIds[i] = jobsScenarioBuilder.takeJob(jobId).getTaskByIndex(0).getTask().getId();
        }

        // Jobs
        JobQuery.Builder jobQueryBuilder = JobQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByCellV3")
                .setPage(PAGE);
        JobQueryResult jobs1 = client.findJobs(jobQueryBuilder
                .putFilteringCriteria("attributes", "titus.cell,titus.stack")
                .putFilteringCriteria("attributes.op", "or")
                .build()
        );
        assertContainsJobs(jobs1, expectedJobIds);
        jobs1.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        JobQueryResult jobs2 = client.findJobs(jobQueryBuilder
                .putFilteringCriteria("attributes", "titus.cell")
                .putFilteringCriteria("attributes.op", "and")
                .build()
        );
        assertContainsJobs(jobs2, expectedJobIds);
        jobs2.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        JobQueryResult jobs3 = client.findJobs(jobQueryBuilder
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s,titus.stack:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "or")
                .build()
        );
        assertContainsJobs(jobs3, expectedJobIds);
        jobs3.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        JobQueryResult jobs4 = client.findJobs(jobQueryBuilder
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "and")
                .build()
        );
        assertContainsJobs(jobs4, expectedJobIds);
        jobs4.getItemsList().forEach(job -> assertCellInfo(job, EmbeddedTitusMaster.CELL_NAME));

        // Tasks
        TaskQuery.Builder taskQueryBuilder = TaskQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testSearchByCellV3")
                .setPage(PAGE);
        TaskQueryResult tasks1 = client.findTasks(taskQueryBuilder
                .putFilteringCriteria("attributes", "titus.cell,titus.stack")
                .putFilteringCriteria("attributes.op", "or")
                .build()
        );
        assertContainsTasks(tasks1, expectedTaskIds);
        tasks1.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));

        TaskQueryResult tasks2 = client.findTasks(taskQueryBuilder
                .putFilteringCriteria("attributes", "titus.cell")
                .putFilteringCriteria("attributes.op", "and")
                .build()
        );
        assertContainsTasks(tasks2, expectedTaskIds);
        tasks2.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));

        TaskQueryResult tasks3 = client.findTasks(taskQueryBuilder
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s,titus.stack:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "or")
                .build()
        );
        assertContainsTasks(tasks3, expectedTaskIds);
        tasks3.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));

        final TaskQueryResult tasks4 = client.findTasks(taskQueryBuilder
                .putFilteringCriteria("attributes",
                        String.format("titus.cell:%1$s", EmbeddedTitusMaster.CELL_NAME))
                .putFilteringCriteria("attributes.op", "and")
                .build()
        );
        assertContainsTasks(tasks4, expectedTaskIds);
        tasks4.getItemsList().forEach(task -> assertCellInfo(task, EmbeddedTitusMaster.CELL_NAME));
    }

    @Test(timeout = 30_000)
    public void testPagination() {
        // We have 3 batch and 3 service jobs.
        Page firstPageOf5 = Page.newBuilder().setPageNumber(0).setPageSize(5).build();
        Page secondPageOf5 = Page.newBuilder().setPageNumber(1).setPageSize(5).build();

        // Jobs
        JobQuery.Builder jobQueryBuilder = JobQuery.newBuilder().putFilteringCriteria("attributes", PRE_CREATED_JOBS_LABEL);

        JobQueryResult jobQueryResult = client.findJobs(jobQueryBuilder.setPage(firstPageOf5).build());
        assertThat(jobQueryResult.getItemsList()).hasSize(5);
        checkPage(jobQueryResult.getPagination(), firstPageOf5, 2, 6, true);

        JobQueryResult jobQueryResult2 = client.findJobs(jobQueryBuilder.setPage(secondPageOf5).build());
        assertThat(jobQueryResult2.getItemsList()).hasSize(1);
        checkPage(jobQueryResult2.getPagination(), secondPageOf5, 2, 6, false);

        Set<String> foundJobIds = new HashSet<>();
        jobQueryResult.getItemsList().forEach(j -> foundJobIds.add(j.getId()));
        jobQueryResult2.getItemsList().forEach(j -> foundJobIds.add(j.getId()));
        assertThat(foundJobIds).hasSize(6);

        // Tasks
        TaskQuery.Builder taskQueryBuilder = TaskQuery.newBuilder().putFilteringCriteria("attributes", PRE_CREATED_JOBS_LABEL);
        TaskQueryResult taskQueryResult = client.findTasks(taskQueryBuilder.setPage(firstPageOf5).build());
        assertThat(taskQueryResult.getItemsList()).hasSize(5);
        checkPage(taskQueryResult.getPagination(), firstPageOf5, 2, 6, true);

        TaskQueryResult taskQueryResult2 = client.findTasks(taskQueryBuilder.setPage(secondPageOf5).build());
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
    public void testFieldsFiltering() {
        JobDescriptor<BatchJobExt> jobDescriptor = BATCH_JOB_TEMPLATE.toBuilder()
                .withApplicationName("testFieldsFiltering")
                .withAttributes(ImmutableMap.of("keyA", "valueA", "keyB", "valueB"))
                .build();
        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
        );

        // Check jobs
        List<Job> foundJobs = client.findJobs(JobQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testFieldsFiltering")
                .addFields("status")
                .addFields("jobDescriptor.attributes.keyA")
                .setPage(PAGE)
                .build()
        ).getItemsList();
        assertThat(foundJobs).hasSize(1);
        assertThat(foundJobs.get(0).getId()).isNotEmpty(); // Always present
        assertThat(foundJobs.get(0).getStatus().getReasonMessage()).isNotEmpty();
        com.netflix.titus.grpc.protogen.JobDescriptor foundJobDescriptor = foundJobs.get(0).getJobDescriptor();
        assertThat(foundJobDescriptor.getAttributesMap()).isNotEmpty();
        assertThat(foundJobDescriptor.getAttributesMap()).containsEntry("keyA", "valueA");

        // Check tasks
        List<Task> foundTasks = client.findTasks(TaskQuery.newBuilder()
                .putFilteringCriteria("applicationName", "testFieldsFiltering")
                .addFields("status")
                .addFields("statusHistory")
                .setPage(PAGE)
                .build()
        ).getItemsList();
        assertThat(foundTasks).hasSize(1);
        assertThat(foundTasks.get(0).getId()).isNotEmpty(); // Always present
        assertThat(foundTasks.get(0).getStatus().getReasonMessage()).isNotEmpty();
        assertThat(foundTasks.get(0).getStatusHistoryList()).isNotEmpty();
        assertThat(foundTasks.get(0).getTaskContextMap()).isEmpty();
    }

    private JobQuery newJobQuery(String... criteria) {
        return JobQuery.newBuilder()
                .putAllFilteringCriteria(CollectionsExt.asMap(criteria))
                .setPage(PAGE)
                .build();
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
