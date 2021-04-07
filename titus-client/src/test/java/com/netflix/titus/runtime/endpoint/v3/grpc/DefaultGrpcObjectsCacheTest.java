package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultGrpcObjectsCacheTest {

    private final GrpcObjectsCacheConfiguration configuration = mock(GrpcObjectsCacheConfiguration.class);

    private final V3JobOperations v3JobOperations = mock(V3JobOperations.class);

    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo = EmptyLogStorageInfo.empty();

    private DefaultGrpcObjectsCache grpcObjectsCache;

    private final SomeJobs someJobs = new SomeJobs();

    @Before
    public void setUp() {
        when(configuration.getCacheRefreshInitialDelayMs()).thenReturn(1L);
        when(configuration.getCacheRefreshIntervalMs()).thenReturn(1L);
        when(configuration.getCacheCleanupDelayMs()).thenReturn(1L);

        someJobs.addJob(2);
        someJobs.addJob(5);
        someJobs.addJob(10);
        someJobs.addJob(20);
        someJobs.addJob(50);
        someJobs.resetMock();
    }

    private void setupGrpcObjectCache(boolean isEnabled) {
        when(configuration.isGrpcObjectsCacheEnabled()).thenReturn(isEnabled);
        grpcObjectsCache = new DefaultGrpcObjectsCache(v3JobOperations, configuration, logStorageInfo, TitusRuntimes.internal());
        grpcObjectsCache.activate();
    }

    @After
    public void tearDown() {
        Evaluators.acceptNotNull(grpcObjectsCache, DefaultGrpcObjectsCache::shutdown);
    }

    private void verifyTasksCacheBehavior(boolean isEnabled, int numCoreTasks) {
        setupGrpcObjectCache(isEnabled);

        List<Task> coreTasks = v3JobOperations.getTasks();

        List<com.netflix.titus.grpc.protogen.Task> grpcTasks1 = coreTasks.stream().map(grpcObjectsCache::getTask).collect(Collectors.toList());
        assertThat(grpcTasks1.size()).isEqualTo(coreTasks.size());

        List<com.netflix.titus.grpc.protogen.Task> grpcTasks2 = coreTasks.stream().map(grpcObjectsCache::getTask).collect(Collectors.toList());
        assertThat(grpcTasks2.size()).isEqualTo(coreTasks.size());

        for (int i = 0; i < grpcTasks1.size(); i++) {
            assertThat(grpcTasks1.get(i) == grpcTasks2.get(i)).isEqualTo(isEnabled);
        }
        if (isEnabled) {
            assertThat(grpcObjectsCache.taskCache.cache.size()).isEqualTo(numCoreTasks);
        } else {
            assertThat(grpcObjectsCache.taskCache.cache.size()).isEqualTo(0);
        }
    }

    private void verifyJobsCacheBehavior(boolean isEnabled, int numCoreJobs) {
        setupGrpcObjectCache(isEnabled);

        List<Job> coreJobs = v3JobOperations.getJobs();

        List<com.netflix.titus.grpc.protogen.Job> grpcJobs1 = coreJobs.stream().map(grpcObjectsCache::getJob).collect(Collectors.toList());
        assertThat(grpcJobs1.size()).isEqualTo(coreJobs.size());

        List<com.netflix.titus.grpc.protogen.Job> grpcJobs2 = coreJobs.stream().map(grpcObjectsCache::getJob).collect(Collectors.toList());
        assertThat(grpcJobs2.size()).isEqualTo(coreJobs.size());

        for (int i = 0; i < grpcJobs1.size(); i++) {
            assertThat(grpcJobs1.get(i) == grpcJobs2.get(i)).isEqualTo(isEnabled);
        }
        if (isEnabled) {
            assertThat(grpcObjectsCache.jobCache.cache.size()).isEqualTo(numCoreJobs);
        } else {
            assertThat(grpcObjectsCache.jobCache.cache.size()).isEqualTo(0);
        }
    }

    @Test
    public void jobsCacheEnabled() {
        verifyJobsCacheBehavior(true, 5);
    }

    @Test
    public void jobsCacheDisabled() {
        verifyJobsCacheBehavior(false, 5);
    }

    @Test
    public void jobCleanup() {
        setupGrpcObjectCache(true);

        Job job = someJobs.getJobs().get(0);
        grpcObjectsCache.getJob(job);
        assertThat(grpcObjectsCache.jobCache.cache.containsKey(job.getId())).isTrue();

        someJobs.removeJob(job.getId());
        someJobs.resetMock();
        await().until(() -> !grpcObjectsCache.jobCache.cache.containsKey(job.getId()));
    }

    @Test
    public void tasksCacheEnabled() {
        verifyTasksCacheBehavior(true, 87);
    }

    @Test
    public void tasksCacheDisabled() {
        verifyTasksCacheBehavior(false, 87);
    }

    @Test
    public void taskCleanup() {
        setupGrpcObjectCache(true);

        Job job = someJobs.getJobs().get(0);
        List<Task> tasks = someJobs.getTasks(job.getId());
        Task task0 = tasks.get(0);
        grpcObjectsCache.getTask(task0);
        assertThat(grpcObjectsCache.taskCache.cache.containsKey(task0.getId())).isTrue();

        someJobs.removeJob(job.getId());
        someJobs.resetMock();
        await().until(() -> !grpcObjectsCache.taskCache.cache.containsKey(task0.getId()));
    }

    class SomeJobs {
        private final Map<String, Pair<Job, List<Task>>> jobsAndTasks = new HashMap<>();

        void addJob(int numTasks) {
            Job job = JobGenerator.oneBatchJob();
            List tasks = JobGenerator.batchTasks(job).getValues(numTasks);
            jobsAndTasks.put(job.getId(), Pair.of(job, tasks));
        }

        void removeJob(String jobId) {
            jobsAndTasks.remove(jobId);
        }

        List<Job> getJobs() {
            return jobsAndTasks.values().stream().map(Pair::getLeft).collect(Collectors.toList());
        }

        List<Task> getTasks(String jobId) {
            return jobsAndTasks.get(jobId).getRight();
        }

        List<Task> getTasks() {
            return jobsAndTasks.values().stream().flatMap(p -> p.getRight().stream()).collect(Collectors.toList());
        }

        void resetMock() {
            when(v3JobOperations.getJobs()).thenReturn(getJobs());
            when(v3JobOperations.getTasks()).thenReturn(getTasks());
        }
    }
}