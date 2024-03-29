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

package com.netflix.titus.ext.cassandra.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.PlatformSidecar;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.volume.SharedContainerVolumeSource;
import com.netflix.titus.api.jobmanager.model.job.volume.Volume;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.junit.category.IntegrationNotParallelizableTest;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import rx.Completable;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Category(IntegrationNotParallelizableTest.class)
public class CassandraJobStoreTest {

    public static final int MAX_CONCURRENCY = 10;
    private static final long STARTUP_TIMEOUT_MS = 30_000L;
    private static final int INITIAL_BUCKET_COUNT = 1;
    private static final int MAX_BUCKET_SIZE = 10;
    /**
     * As Cassandra uses memory mapped files there are sometimes issues with virtual disks storing the project files.
     * To solve this issue, we relocate the default embedded Cassandra folder to /var/tmp/embeddedCassandra.
     */
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";
    private static final CassandraStoreConfiguration CONFIGURATION = new TestCassandraStoreConfiguration();
    @Rule
    public CassandraCQLUnit cassandraCqlUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("tables.cql", "titus_integration_tests"),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT_MS
    );

    @Test
    public void testRetrieveJobs() {
        Session session = cassandraCqlUnit.getSession();
        JobStore bootstrappingStore = getJobStore(session);
        Job<BatchJobExt> job = createBatchJobObject();
        bootstrappingStore.storeJob(job).await();
        JobStore store = getJobStore(session);
        store.init().await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft()).hasSize(1);
        assertThat(jobsAndErrors.getRight()).isEqualTo(0);
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));

        // Check that archive access does not return anything.
        try {
            store.retrieveArchivedJob(job.getId()).toBlocking().first();
            fail("Should not return active job");
        } catch (JobStoreException e) {
            assertThat(e.getErrorCode()).isEqualTo(JobStoreException.ErrorCode.JOB_DOES_NOT_EXIST);
        }
    }

    @Test
    public void testRetrieveBatchJob() {
        doRetrieveJob(createBatchJobObject());
    }

    @Test
    public void testRetrieveServiceJob() {
        doRetrieveJob(createServiceJobObject());
    }

    @Test
    public void testRetrieveJobWithVolumes() {
        doRetrieveJob(createServiceJobWithVolumesObject());
    }

    @Test
    public void testPlatformSidecarJob() {
        doRetrieveJob(createServiceJobWithPlatformSidecarsObject());
    }

    private <E extends JobDescriptor.JobDescriptorExt> void doRetrieveJob(Job<E> job) {
        JobStore store = getJobStore();
        store.storeJob(job).await();
        store.init().await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
    }

    @Test
    public void testStoreJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
    }

    /**
     * Create enough jobs to evenly be bucketed across multiple rows. Delete 1 job per bucket. Add back enough jobs to fill
     * in the deleted jobs plus an extra bucket worth as a new bucket was created when reaching the max of all the original buckets.
     */
    @Test
    public void testActiveJobIdDistribution() {
        int numberOfJobsToCreate = 100;
        int numberOfBuckets = numberOfJobsToCreate / MAX_BUCKET_SIZE;
        Session session = cassandraCqlUnit.getSession();
        JobStore store = getJobStore(session);
        store.init().await();
        List<Job<?>> createdJobs = new ArrayList<>();
        List<Completable> completables = new ArrayList<>();
        for (int i = 0; i < numberOfJobsToCreate; i++) {
            Job<BatchJobExt> job = createBatchJobObject();
            createdJobs.add(job);
            completables.add(store.storeJob(job));
        }
        Completable.merge(Observable.from(completables), MAX_CONCURRENCY).await();
        Pair<List<Job<?>>, Integer> retrievedJobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(retrievedJobsAndErrors.getLeft()).hasSize(numberOfJobsToCreate);
        assertItemsPerBucket(session, numberOfBuckets, MAX_BUCKET_SIZE);

        int j = 0;
        int jobsRemoved = 0;
        completables = new ArrayList<>();
        while (j < numberOfJobsToCreate) {
            Job<?> jobToRemove = createdJobs.get(j);
            completables.add(store.deleteJob(jobToRemove));
            j += MAX_BUCKET_SIZE;
            jobsRemoved++;
        }
        Completable.merge(Observable.from(completables), MAX_CONCURRENCY).await();
        assertItemsPerBucket(session, numberOfBuckets, MAX_BUCKET_SIZE - 1);

        completables = new ArrayList<>();
        for (int i = 0; i < jobsRemoved + MAX_BUCKET_SIZE; i++) {
            Job<BatchJobExt> job = createBatchJobObject();
            completables.add(store.storeJob(job));
        }
        Completable.merge(Observable.from(completables), MAX_CONCURRENCY).await();
        retrievedJobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(retrievedJobsAndErrors.getLeft()).hasSize(numberOfJobsToCreate + MAX_BUCKET_SIZE);
        assertItemsPerBucket(session, numberOfBuckets + 1, MAX_BUCKET_SIZE);
    }

    private void assertItemsPerBucket(Session session, int numberOfBuckets, int expectedNumberOfItemsPerBucket) {
        for (int i = 0; i < numberOfBuckets; i++) {
            ResultSet resultSet = session.execute("SELECT COUNT(*) FROM active_job_ids WHERE bucket = " + i);
            long numberOfItemsInBucket = resultSet.one().getLong(0);
            assertThat(numberOfItemsInBucket).isEqualTo(expectedNumberOfItemsPerBucket);
        }
    }

    @Test
    public void testUpdateJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Job<BatchJobExt> newJob = job.toBuilder()
                .withStatus(JobStatus.newBuilder().withState(JobState.Finished).build())
                .build();
        store.updateJob(newJob).await();
        Pair<List<Job<?>>, Integer> newJobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(newJob, newJobsAndErrors.getLeft().get(0));
    }

    @Test
    public void testDeleteJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        store.deleteJob(job).await();
        jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft()).isEmpty();
    }

    @Test
    public void testRetrieveTasksForJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Pair<List<Task>, Integer> tasks = store.retrieveTasksForJob(job.getId()).toBlocking().first();
        checkRetrievedTask(task, tasks.getLeft().get(0));

        // Check that archive access does not return anything.
        Task archivedTask = store.retrieveArchivedTasksForJob(job.getId()).toBlocking().firstOrDefault(null);
        assertThat(archivedTask).isNull();
    }

    @Test
    public void testRetrieveTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        checkRetrievedTask(task, retrievedTask);

        // Check that archive access does not return anything.
        try {
            store.retrieveArchivedTask(task.getId()).toBlocking().first();
            fail("Should not return active task");
        } catch (JobStoreException e) {
            assertThat(e.getErrorCode()).isEqualTo(JobStoreException.ErrorCode.TASK_DOES_NOT_EXIST);
        }
    }

    @Test
    public void testStoreTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        checkRetrievedTask(task, retrievedTask);
    }

    @Test
    public void testUpdateTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        checkRetrievedTask(task, retrievedTask);
        BatchJobTask newTask = BatchJobTask.newBuilder((BatchJobTask) task)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build())
                .build();
        store.updateTask(newTask).await();
        Task newRetrievedTask = store.retrieveTask(newTask.getId()).toBlocking().first();
        checkRetrievedTask(newTask, newRetrievedTask);
    }

    @Test
    public void testReplaceTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task firstTask = createTaskObject(job);
        store.storeTask(firstTask).await();
        Task retrievedTask = store.retrieveTask(firstTask.getId()).toBlocking().first();
        checkRetrievedTask(firstTask, retrievedTask);
        Task secondTask = createTaskObject(job);
        store.replaceTask(firstTask, secondTask).await();
        Pair<List<Task>, Integer> tasks = store.retrieveTasksForJob(job.getId()).toBlocking().first();
        assertThat(tasks.getLeft()).hasSize(1);
        assertThat(tasks.getRight()).isEqualTo(0);
        checkRetrievedTask(secondTask, tasks.getLeft().get(0));
    }

    @Test
    public void testDeleteTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        checkRetrievedTask(task, retrievedTask);
        store.deleteTask(task).await();
        Pair<List<Task>, Integer> tasks = store.retrieveTasksForJob(job.getId()).toBlocking().first();
        assertThat(tasks.getLeft()).isEmpty();
    }

    @Test
    public void testRetrieveArchivedJob() {
        testRetrieveArchivedJob(true);
    }

    @Test
    public void testRetrieveArchivedJobFromActiveTable() {
        testRetrieveArchivedJob(false);
    }

    private void testRetrieveArchivedJob(boolean archive) {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createFinishedBatchJobObject();

        store.init().await();
        store.storeJob(job).await();
        if (archive) {
            store.deleteJob(job).await();
        }
        Job archivedJob = store.retrieveArchivedJob(job.getId()).toBlocking().first();
        checkRetrievedJob(job, archivedJob);
    }

    @Test
    public void testRetrieveArchivedTasksForJob() {
        testRetrieveArchivedTasksForJob(true);
    }

    @Test
    public void testRetrieveArchivedTasksForJobFromActiveTable() {
        testRetrieveArchivedTasksForJob(false);
    }

    private void testRetrieveArchivedTasksForJob(boolean archive) {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createFinishedBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createFinishedTaskObject(job);
        store.storeTask(task).await();
        if (archive) {
            store.deleteTask(task).await();
        }
        Task archivedTask = store.retrieveArchivedTasksForJob(job.getId()).toBlocking().first();
        checkRetrievedTask(task, archivedTask);
    }

    @Test
    public void testRetrieveArchivedTask() {
        testRetrieveArchivedTask(true);
    }

    @Test
    public void testRetrieveArchivedTaskFromActiveTable() {
        testRetrieveArchivedTask(false);
    }

    private void testRetrieveArchivedTask(boolean archive) {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createFinishedBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createFinishedTaskObject(job);
        store.storeTask(task).await();
        if (archive) {
            store.deleteTask(task).await();
        }
        Task archivedTask = store.retrieveArchivedTask(task.getId()).toBlocking().first();
        checkRetrievedTask(task, archivedTask);
    }

    @Test
    public void testMoveTask() {
        JobStore store = getJobStore();
        store.init().await();

        Job<ServiceJobExt> jobFrom = createServiceJobObject();
        store.storeJob(jobFrom).await();

        Job<ServiceJobExt> jobTo = createServiceJobObject();
        store.storeJob(jobTo).await();

        Task task = createServiceTaskObject(jobFrom);
        store.storeTask(task).await();

        Job<ServiceJobExt> updatedFromJob = JobFunctions.incrementJobSize(jobFrom, -1);
        Job<ServiceJobExt> updatedToJob = JobFunctions.incrementJobSize(jobTo, 1);
        Task updatedTask = task.toBuilder().withJobId(updatedToJob.getId()).build();
        store.moveTask(updatedFromJob, updatedToJob, updatedTask).await();

        // Load jobFrom from store
        Job<?> jobFromLoaded = store.retrieveJob(jobFrom.getId()).toBlocking().first();
        assertThat(JobFunctions.getJobDesiredSize(jobFromLoaded)).isEqualTo(0);

        Pair<List<Task>, Integer> jobFromTasksLoaded = store.retrieveTasksForJob(jobFrom.getId()).toBlocking().first();
        assertThat(jobFromTasksLoaded.getLeft()).hasSize(0);

        // Load jobTo from store
        Job<?> jobToLoaded = store.retrieveJob(jobTo.getId()).toBlocking().first();
        assertThat(JobFunctions.getJobDesiredSize(jobToLoaded)).isEqualTo(2);

        Pair<List<Task>, Integer> jobToTasksLoaded = store.retrieveTasksForJob(jobTo.getId()).toBlocking().first();
        assertThat(jobToTasksLoaded.getLeft()).hasSize(1);
        jobToTasksLoaded.getLeft().forEach(t -> assertThat(t.getJobId()).isEqualTo(jobTo.getId()));
    }

    @Test
    public void testRetrieveArchivedTaskCountForJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createFinishedBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createFinishedTaskObject(job);
        store.storeTask(task).await();
        store.deleteTask(task).await();
        Long count = store.retrieveArchivedTaskCountForJob(job.getId()).toBlocking().first();
        assertThat(count).isEqualTo(1);
    }

    @Test
    public void testDeleteArchivedTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createFinishedBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        checkRetrievedJob(job, jobsAndErrors.getLeft().get(0));
        Task task = createFinishedTaskObject(job);
        store.storeTask(task).await();
        store.deleteTask(task).await();
        Long count = store.retrieveArchivedTaskCountForJob(job.getId()).toBlocking().first();
        assertThat(count).isEqualTo(1);
        store.deleteArchivedTask(job.getId(), task.getId()).await();
        Long count2 = store.retrieveArchivedTaskCountForJob(job.getId()).toBlocking().first();
        assertThat(count2).isEqualTo(0);
    }

    private JobStore getJobStore() {
        return getJobStore(null);
    }

    private JobStore getJobStore(Session session) {
        if (session == null) {
            session = cassandraCqlUnit.getSession();
        }
        return new CassandraJobStore(CONFIGURATION, session, TitusRuntimes.internal(),
                ObjectMappers.storeMapper(), INITIAL_BUCKET_COUNT, MAX_BUCKET_SIZE);
    }

    private Job<BatchJobExt> createBatchJobObject() {
        return JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue();
    }

    private Job createFinishedBatchJobObject() {
        return JobFunctions.changeJobStatus(
                createBatchJobObject(),
                JobStatus.newBuilder().withState(JobState.Finished).build()
        );
    }

    private Job<ServiceJobExt> createServiceJobObject() {
        ExponentialBackoffRetryPolicy exponential = JobModel.newExponentialBackoffRetryPolicy()
                .withInitialDelayMs(10)
                .withMaxDelayMs(100)
                .withRetries(5)
                .build();
        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor().but(jd ->
                jd.getExtensions().toBuilder().withRetryPolicy(exponential).build()
        );
        return JobGenerator.serviceJobs(jobDescriptor).getValue();
    }

    private Job<ServiceJobExt> createServiceJobWithVolumesObject() {
        List<Volume> volumes = Collections.singletonList(createTestVolume());
        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor().but(jd ->
                jd.toBuilder().withVolumes(volumes).build());
        return JobGenerator.serviceJobs(jobDescriptor).getValue();
    }

    /**
     * createServiceJobWithPlatformSidecarsObject is an extra strenuous test for the CassandraJobStore
     * suite, as it exercises all the things needed to ensure that the complex arguments field
     * is properly serialized correctly.
     */
    private Job<ServiceJobExt> createServiceJobWithPlatformSidecarsObject() {
        Struct.Builder args = Struct.newBuilder();
        args.putFields("foo", Value.newBuilder().setStringValue("bar").build());
        args.putFields("baz", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        args.putFields("num", Value.newBuilder().setNumberValue(42.0).build());
        PlatformSidecar ps1 = PlatformSidecar.newBuilder()
                .withName("testSidecar")
                .withChannel("testChannel")
                .withArguments("{\"foo\":true,\"bar\":3.0}")
                .build();
        List<PlatformSidecar> platformSidecars = Collections.singletonList(ps1);
        JobDescriptor<ServiceJobExt> jobDescriptor = JobDescriptorGenerator.oneTaskServiceJobDescriptor().but(jd ->
                jd.toBuilder().withPlatformSidecars(platformSidecars).build()
        );
        return JobGenerator.serviceJobs(jobDescriptor).getValue();
    }

    private Volume createTestVolume() {
        SharedContainerVolumeSource volumeSource = new SharedContainerVolumeSource("sourceContainer", "sourcePath");
        return new Volume("testVolume", volumeSource);
    }

    private Task createTaskObject(Job<BatchJobExt> job) {
        return JobGenerator.oneBatchTask().toBuilder().withJobId(job.getId()).build();
    }

    private Task createFinishedTaskObject(Job<BatchJobExt> job) {
        return JobFunctions.changeTaskStatus(createTaskObject(job), TaskStatus.newBuilder().withState(TaskState.Finished).build());
    }

    private Task createServiceTaskObject(Job<ServiceJobExt> job) {
        String taskId = UUID.randomUUID().toString();
        return ServiceJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .withStatus(TaskStatus.newBuilder()
                        .withState(TaskState.Accepted)
                        .withTimestamp(System.currentTimeMillis())
                        .build()
                )
                .build();
    }

    private void checkRetrievedJob(Job<?> job, Job<?> retrievedJob) {
        if (job.getVersion().equals(Version.undefined())) {
            assertThat(retrievedJob.getVersion().getTimestamp()).isEqualTo(job.getStatus().getTimestamp());
            assertThat(retrievedJob.toBuilder().withVersion(Version.undefined()).build()).isEqualTo(job);
        } else {
            assertThat(retrievedJob).isEqualTo(job);
        }
    }

    private void checkRetrievedTask(Task task, Task retrievedTask) {
        if (task.getVersion().equals(Version.undefined())) {
            assertThat(retrievedTask.getVersion().getTimestamp()).isEqualTo(task.getStatus().getTimestamp());
            assertThat(retrievedTask.toBuilder().withVersion(Version.undefined()).build()).isEqualTo(task);
        } else {
            assertThat(retrievedTask).isEqualTo(task);
        }
    }
}