package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import javax.annotation.PreDestroy;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.grpc.protogen.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcObjectsCache {
    private static final Logger logger = LoggerFactory.getLogger(GrpcObjectsCache.class);

    private static final int MAX_CACHE_SIZE_JOBS = 50_000;
    private final LoadingCache<com.netflix.titus.api.jobmanager.model.job.Job<?>, Job> jobs;
    private final ScheduleReference schedulerRef;
    private final ExecutorService executorService;

    public GrpcObjectsCache(TitusRuntime titusRuntime, GrpcObjectsCacheConfiguration configuration) {
        ScheduleDescriptor gcScheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName("")
                .withDescription("")
                .withInitialDelay(Duration.ofMillis(configuration.getCacheRefreshInitialDelayMs()))
                .withInterval(Duration.ofMillis(configuration.getCacheRefreshIntervalMs()))
                .withTimeout(Duration.ofMillis(configuration.getCacheRefreshTimeoutMs()))
                .build();

        executorService = ExecutorsExt.namedSingleThreadExecutor("GrpcObjectsCacheRefresh");
        schedulerRef = titusRuntime.getLocalScheduler().schedule(gcScheduleDescriptor, e -> refreshCache(), executorService);


        jobs = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE_JOBS)
                .build(this::toGrpcJob);
    }

    private void refreshCache() {

    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(executorService, ExecutorService::shutdown);
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }

    public Job getJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        return jobs.get(coreJob);
    }

    private Job toGrpcJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        Job job = GrpcJobManagementModelConverters.toGrpcJob(coreJob);
        serializeJob(job);
        return job;
    }

    private void serializeJob(Job job) {
        int jobSerializedSize = job.getSerializedSize();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(jobSerializedSize);
        try {
            job.writeTo(bos);
            logger.debug("Serialized size for job object {}", jobSerializedSize);
        } catch (IOException e) {
            logger.error("Error serializing grpc JobType ", e);
        }
    }


}
