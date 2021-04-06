package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.protobuf.GeneratedMessageV3;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultGrpcObjectsCache implements GrpcObjectsCache {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGrpcObjectsCache.class);

    private static final int MAX_CACHE_SIZE_JOBS = 50_000;
    private static final int MAX_CACHE_SIZE_TASKS = 50_000;

    protected final LoadingCache<com.netflix.titus.api.jobmanager.model.job.Job<?>, Job> jobs;
    protected final LoadingCache<com.netflix.titus.api.jobmanager.model.job.Task, Task> tasks;

    private ScheduleReference schedulerRef;
    private ExecutorService executorService;
    private final V3JobOperations jobOperations;
    private final TitusRuntime titusRuntime;
    private final GrpcObjectsCacheConfiguration configuration;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;

    @Inject
    public DefaultGrpcObjectsCache(V3JobOperations jobOperations, TitusRuntime titusRuntime,
                                   GrpcObjectsCacheConfiguration configuration,
                                   LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo) {
        this.titusRuntime = titusRuntime;
        this.configuration = configuration;
        this.logStorageInfo = logStorageInfo;
        this.jobOperations = jobOperations;

        // Initialize cache
        jobs = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE_JOBS)
                .build(this::toGrpcJob);

        tasks = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE_TASKS)
                .build(this::toGrpcTask);
    }

    @Activator
    public void activate() {
        ScheduleDescriptor gcScheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName("")
                .withDescription("")
                .withInitialDelay(Duration.ofMillis(configuration.getCacheRefreshInitialDelayMs()))
                .withInterval(Duration.ofMillis(configuration.getCacheRefreshIntervalMs()))
                .withTimeout(Duration.ofMillis(configuration.getCacheRefreshTimeoutMs()))
                .build();
        executorService = ExecutorsExt.namedSingleThreadExecutor("GrpcObjectsCacheRefresh");
        schedulerRef = titusRuntime.getLocalScheduler().schedule(gcScheduleDescriptor, e -> refreshCache(), executorService);
    }


    private void refreshCache() {
        logger.debug("Refreshing cache");
        Set<com.netflix.titus.api.jobmanager.model.job.Job> activeJobs = new HashSet<>(jobOperations.getJobs());
        List<com.netflix.titus.api.jobmanager.model.job.Job<?>> inActiveJobs = jobs.asMap().keySet().stream().filter(job -> !activeJobs.contains(job)).collect(Collectors.toList());
        inActiveJobs.forEach(jobs::invalidate);
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(executorService, ExecutorService::shutdown);
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }

    public Job getJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        if (configuration.isGrpcObjectsCacheEnabled()) {
            return jobs.get(coreJob);
        } else {
            return toGrpcJob(coreJob);
        }
    }

    public Task getTask(com.netflix.titus.api.jobmanager.model.job.Task coreTask) {
        if (configuration.isGrpcObjectsCacheEnabled()) {
            return tasks.get(coreTask);
        } else {
            return toGrpcTask(coreTask);
        }
    }

    private Task toGrpcTask(com.netflix.titus.api.jobmanager.model.job.Task coreTask) {
        Task task = GrpcJobManagementModelConverters.toGrpcTask(coreTask, logStorageInfo);
        // Need to serialize it once since GeneratedMessageV3 isn't entirely immutable.
        // It needs to initialize memoizedSize field (non-volatile) before being used from multiple threads
        serializeGrpcObject(task);
        return task;
    }

    private Job toGrpcJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        Job job = GrpcJobManagementModelConverters.toGrpcJob(coreJob);
        // Need to serialize it once since GeneratedMessageV3 isn't entirely immutable.
        // It needs to initialize memoizedSize field (non-volatile) before being used from multiple threads
        serializeGrpcObject(job);
        return job;
    }

    private <T extends GeneratedMessageV3> void serializeGrpcObject(T msg) {
        int serializedSize = msg.getSerializedSize();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(serializedSize);
        try {
            msg.writeTo(bos);
            logger.debug("Serialized size for grpc type {}", serializedSize);
        } catch (IOException e) {
            logger.error("Error serializing grpc type ", e);
        }
    }
}
