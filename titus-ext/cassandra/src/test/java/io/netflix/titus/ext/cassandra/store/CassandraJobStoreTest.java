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

package io.netflix.titus.ext.cassandra.store;

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Session;
import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import io.netflix.titus.testkit.model.job.JobGenerator;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class CassandraJobStoreTest {

    private static final long STARTUP_TIMEOUT = 30_000L;

    /**
     * As Cassandra uses memory mapped files there are sometimes issues with virtual disks storing the project files.
     * To solve this issue, we relocate the default embedded Cassandra folder to /var/tmp/embeddedCassandra.
     */
    private static final String CONFIGURATION_FILE_NAME = "relocated-cassandra.yaml";

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("tables.cql", "titus_integration_tests"),
            CONFIGURATION_FILE_NAME,
            STARTUP_TIMEOUT
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
        public int getConcurrencyLimit() {
            return 10;
        }
    };

    @Test
    public void testRetrieveJobs() throws Exception {
        Session session = cassandraCQLUnit.getSession();
        JobStore bootstrappingStore = new CassandraJobStore(CONFIGURATION, session, ObjectMappers.storeMapper());
        Job<BatchJobExt> job = createBatchJobObject();
        bootstrappingStore.storeJob(job).await();
        JobStore store = new CassandraJobStore(CONFIGURATION, session, ObjectMappers.storeMapper());
        store.init().await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0)).isEqualTo(job);
    }

    @Test
    public void testRetrieveBatchJob() throws Exception {
        doRetrieveJob(createBatchJobObject());
    }

    @Test
    public void testRetrieveServiceJob() throws Exception {
        doRetrieveJob(createServiceJobObject());
    }

    private <E extends JobDescriptor.JobDescriptorExt> void doRetrieveJob(Job<E> job) {
        JobStore store = getJobStore();
        store.storeJob(job).await();
        store.init().await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
    }

    @Test
    public void testStoreJob() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
    }

    @Test
    public void testUpdateJob() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Job<BatchJobExt> newJob = Job.newBuilder(job)
                .withStatus(JobStatus.newBuilder().withState(JobState.Finished).build())
                .build();
        store.updateJob(newJob).await();
        List<Job<?>> newJobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(newJobs.get(0)).isEqualTo(newJob);
    }

    @Test
    public void testDeleteJob() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        store.deleteJob(job).await();
        jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs).isEmpty();
    }

    @Test
    public void testRetrieveTasksForJob() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        List<Task> tasks = store.retrieveTasksForJob(job.getId()).toList().toBlocking().first();
        assertThat(tasks.get(0)).isEqualTo(task);
    }

    @Test
    public void testRetrieveTask() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
    }

    @Test
    public void testStoreTask() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
    }

    @Test
    public void testUpdateTask() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
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
    public void testReplaceTask() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task firstTask = createTaskObject(job);
        store.storeTask(firstTask).await();
        Task retrievedTask = store.retrieveTask(firstTask.getId()).toBlocking().first();
        assertThat(firstTask).isEqualTo(retrievedTask);
        Task secondTask = createTaskObject(job);
        store.replaceTask(firstTask, secondTask).await();
        List<Task> tasks = store.retrieveTasksForJob(job.getId()).toList().toBlocking().first();
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0)).isEqualTo(secondTask);
    }

    @Test
    public void testDeleteTask() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        Task retrievedTask = store.retrieveTask(task.getId()).toBlocking().first();
        assertThat(task).isEqualTo(retrievedTask);
        store.deleteTask(task).await();
        List<Task> tasks = store.retrieveTasksForJob(job.getId()).toList().toBlocking().first();
        assertThat(tasks).isEmpty();
    }

    @Test
    public void testRetrieveArchivedJob() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        store.deleteJob(job).await();
        Job archivedJob = store.retrieveArchivedJob(job.getId()).toBlocking().first();
        assertThat(archivedJob).isEqualTo(job);
    }

    @Test
    public void testRetrieveArchivedTasksForJob() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        store.deleteTask(task).await();
        Task archivedTask = store.retrieveArchivedTasksForJob(job.getId()).toBlocking().first();
        assertThat(archivedTask).isEqualTo(task);
    }

    @Test
    public void testRetrieveArchivedTask() throws Exception {
        JobStore store = getJobStore();
        Job<BatchJobExt> job = createBatchJobObject();
        store.init().await();
        store.storeJob(job).await();
        List<Job<?>> jobs = store.retrieveJobs().toList().toBlocking().first();
        assertThat(jobs.get(0)).isEqualTo(job);
        Task task = createTaskObject(job);
        store.storeTask(task).await();
        store.deleteTask(task).await();
        Task archivedTask = store.retrieveArchivedTask(task.getId()).toBlocking().first();
        assertThat(archivedTask).isEqualTo(task);
    }

    private JobStore getJobStore() {
        Session session = cassandraCQLUnit.getSession();
        return new CassandraJobStore(CONFIGURATION, session, ObjectMappers.storeMapper());
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