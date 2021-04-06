package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultGrpcObjectsCacheTest {

    private void verifyTasksCacheBehavior(boolean isEnabled, int numCoreTasks) {
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        GrpcObjectsCacheConfiguration configuration = mock(GrpcObjectsCacheConfiguration.class);
        when(configuration.isGrpcObjectsCacheEnabled()).thenReturn(isEnabled);
        LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo = EmptyLogStorageInfo.empty();
        DefaultGrpcObjectsCache grpcObjectsCache = new DefaultGrpcObjectsCache(v3JobOperations, TitusRuntimes.internal(), configuration, logStorageInfo);

        List<Task> coreTasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Task task= JobGenerator.oneBatchTask();
            coreTasks.add(task);
        }

        List<com.netflix.titus.grpc.protogen.Task> grpcTasks1 = coreTasks.stream().map(grpcObjectsCache::getTask).collect(Collectors.toList());
        assertThat(grpcTasks1.size()).isEqualTo(coreTasks.size());

        List<com.netflix.titus.grpc.protogen.Task> grpcTasks2 = coreTasks.stream().map(grpcObjectsCache::getTask).collect(Collectors.toList());
        assertThat(grpcTasks2.size()).isEqualTo(coreTasks.size());

        for (int i = 0; i < grpcTasks1.size(); i++) {
            assertThat(grpcTasks1.get(i) == grpcTasks2.get(i)).isEqualTo(isEnabled);
        }
        if (isEnabled) {
            assertThat(grpcObjectsCache.tasks.asMap().size()).isEqualTo(numCoreTasks);
        } else {
            assertThat(grpcObjectsCache.tasks.asMap().size()).isEqualTo(0);
        }
    }


    private void verifyJobsCacheBehavior(boolean isEnabled, int numCoreJobs) {
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        GrpcObjectsCacheConfiguration configuration = mock(GrpcObjectsCacheConfiguration.class);
        when(configuration.isGrpcObjectsCacheEnabled()).thenReturn(isEnabled);
        LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo = mock(LogStorageInfo.class);
        DefaultGrpcObjectsCache grpcObjectsCache = new DefaultGrpcObjectsCache(v3JobOperations, TitusRuntimes.internal(), configuration, logStorageInfo);

        List<Job<?>> coreJobs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Job<BatchJobExt> job = JobGenerator.oneBatchJob();
            coreJobs.add(job);
        }

        List<com.netflix.titus.grpc.protogen.Job> grpcJobs1 = coreJobs.stream().map(grpcObjectsCache::getJob).collect(Collectors.toList());
        assertThat(grpcJobs1.size()).isEqualTo(coreJobs.size());

        List<com.netflix.titus.grpc.protogen.Job> grpcJobs2 = coreJobs.stream().map(grpcObjectsCache::getJob).collect(Collectors.toList());
        assertThat(grpcJobs2.size()).isEqualTo(coreJobs.size());

        for (int i = 0; i < grpcJobs1.size(); i++) {
            assertThat(grpcJobs1.get(i) == grpcJobs2.get(i)).isEqualTo(isEnabled);
        }
        if (isEnabled) {
            assertThat(grpcObjectsCache.jobs.asMap().size()).isEqualTo(numCoreJobs);
        } else {
            assertThat(grpcObjectsCache.jobs.asMap().size()).isEqualTo(0);
        }
    }

    @Test
    public void jobsCacheEnabled() {
        verifyJobsCacheBehavior(true, 10);
    }

    @Test
    public void jobsCacheDisabled() {
        verifyJobsCacheBehavior(false, 5);
    }

    @Test
    public void tasksCacheEnabled() {
        verifyTasksCacheBehavior(true, 10);
    }

    @Test
    public void tasksCacheDisabled() {
        verifyTasksCacheBehavior(false, 5);
    }
}