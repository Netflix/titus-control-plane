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
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
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

@Category(IntegrationTest.class)
public class CassandraJobStoreTest {

    private static final long STARTUP_TIMEOUT_MS = 30_000L;
    private static final int INITIAL_BUCKET_COUNT = 1;
    private static final int MAX_BUCKET_SIZE = 10;

    /**
     * As Cassandra uses memory mapped files there are sometimes issues with virtual disks storing the project files.
     * To solve this issue, we relocate the default embedded Cassandra folder to /var/tmp/embeddedCassandra.
     */
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";
    public static final int MAX_CONCURRENCY = 10;

    @Rule
    public CassandraCQLUnit cassandraCqlUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("tables.cql", "titus_integration_tests"),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT_MS
    );

    private static final CassandraStoreConfiguration CONFIGURATION = new CassandraStoreConfiguration() {
        @Override
        public boolean isFailOnInconsistentAgentData() {
            return true;
        }

        @Override
        public boolean isFailOnInconsistentLoadBalancerData() {
            return false;
        }

        @Override
        public boolean isFailOnInconsistentSchedulerData() {
            return false;
        }

        @Override
        public int getConcurrencyLimit() {
            return 10;
        }

        @Override
        public boolean isTracingEnabled() {
            return false;
        }
    };

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
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
    }

    @Test
    public void testRetrieveBatchJob() {
        doRetrieveJob(createBatchJobObject());
    }

    @Test
    public void testRetrieveServiceJob() {
        doRetrieveJob(createServiceJobObject());
    }

    private <E extends JobDescriptor.JobDescriptorExt> void doRetrieveJob(Job<E> job) {
        JobStore store = getJobStore();
        store.storeJob(job).await();
        store.init().await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
    }

    @Test
    public void testStoreJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
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
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Job<BatchJobExt> newJob = Job.newBuilder(job)
                .withStatus(JobStatus.newBuilder().withState(JobState.Finished).build())
                .build();
        store.updateJob(newJob).await();
        Pair<List<Job<?>>, Integer> newJobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(newJobsAndErrors.getLeft().get(0)).isEqualTo(newJob);
    }

    @Test
    public void testDeleteJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
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
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Pair<List<Task>, Integer> tasks = store.retrieveTasksForJob(job.getId()).toBlocking().first();
        assertThat(tasks.getLeft().get(0)).isEqualTo(task);
    }

    @Test
    public void testRetrieveTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
    }

    @Test
    public void testStoreTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
    }

    @Test
    public void testUpdateTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
        BatchJobTask newTask = BatchJobTask.newBuilder((BatchJobTask) task)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Finished).build())
                .build();
        store.updateTask(newTask).await();
        Task newRetrievedTask = store.retrieveTask(newTask.getId()).toBlocking().first();
        assertThat(newTask).isEqualTo(newRetrievedTask);
    }

    @Test
    public void testReplaceTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task firstTask = createTaskObject(job);
        store.storeTask(firstTask).await();
        Task retrievedTask = store.retrieveTask(firstTask.getId()).toBlocking().first();
        assertThat(firstTask).isEqualTo(retrievedTask);
        Task secondTask = createTaskObject(job);
        store.replaceTask(firstTask, secondTask).await();
        Pair<List<Task>, Integer> tasks = store.retrieveTasksForJob(job.getId()).toBlocking().first();
        assertThat(tasks.getLeft()).hasSize(1);
        assertThat(tasks.getRight()).isEqualTo(0);
        assertThat(tasks.getLeft().get(0)).isEqualTo(secondTask);
    }

    @Test
    public void testDeleteTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
        store.deleteTask(task).await();
        Pair<List<Task>, Integer> tasks = store.retrieveTasksForJob(job.getId()).toBlocking().first();
        assertThat(tasks.getLeft()).isEmpty();
    }

    @Test
    public void testRetrieveArchivedJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        store.deleteJob(job).await();
        Job archivedJob = store.retrieveArchivedJob(job.getId()).toBlocking().first();
        assertThat(archivedJob).isEqualTo(job);
    }

    @Test
    public void testRetrieveArchivedTasksForJob() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        store.deleteTask(task).await();
        Task archivedTask = store.retrieveArchivedTasksForJob(job.getId()).toBlocking().first();
        assertThat(archivedTask).isEqualTo(task);
    }

    @Test
    public void testRetrieveArchivedTask() {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        Pair<List<Job<?>>, Integer> jobsAndErrors = store.retrieveJobs().toBlocking().first();
        assertThat(jobsAndErrors.getLeft().get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        store.deleteTask(task).await();
        Task archivedTask = store.retrieveArchivedTask(task.getId()).toBlocking().first();
        assertThat(archivedTask).isEqualTo(task);
    }

    private JobStore getJobStore() {
        return getJobStore(null);
    }

    private JobStore getJobStore(Session session) {
        if (session == null) {
            session = cassandraCqlUnit.getSession();
        }
        return new CassandraJobStore(CONFIGURATION, session, TitusRuntimes.internal(), ObjectMappers.storeMapper(),
                INITIAL_BUCKET_COUNT, MAX_BUCKET_SIZE);
    }

    private Job<BatchJobExt> createBatchJobObject() {
        return JobGenerator.batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue();
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

    private Task createTaskObject(Job<BatchJobExt> job) {
        String taskId = UUID.randomUUID().toString();
        return BatchJobTask.newBuilder()
                .withId(taskId)
                .withJobId(job.getId())
                .build();
    }
}