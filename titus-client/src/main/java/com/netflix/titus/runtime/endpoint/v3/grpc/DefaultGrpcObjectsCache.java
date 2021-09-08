package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.GeneratedMessageV3;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultGrpcObjectsCache implements GrpcObjectsCache {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGrpcObjectsCache.class);

    protected ProtobufCache<com.netflix.titus.api.jobmanager.model.job.Job<?>, Job> jobCache;
    protected ProtobufCache<com.netflix.titus.api.jobmanager.model.job.Task, Task> taskCache;

    private final V3JobOperations jobOperations;
    private final GrpcObjectsCacheConfiguration configuration;
    private final LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo;
    private final TitusRuntime titusRuntime;

    @Inject
    public DefaultGrpcObjectsCache(V3JobOperations jobOperations,
                                   GrpcObjectsCacheConfiguration configuration,
                                   LogStorageInfo<com.netflix.titus.api.jobmanager.model.job.Task> logStorageInfo,
                                   TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.configuration = configuration;
        this.logStorageInfo = logStorageInfo;
        this.jobOperations = jobOperations;
    }

    @Activator
    public void activate() {
        // Initialize cache
        this.jobCache = new ProtobufCache<>(
                "jobs",
                coreJob -> {
                    Job job = GrpcJobManagementModelConverters.toGrpcJob(coreJob);
                    // Need to serialize it once since GeneratedMessageV3 isn't entirely immutable.
                    // It needs to initialize memoizedSize field (non-volatile) before being used from multiple threads
                    serializeGrpcObject(job);
                    return job;
                },
                () -> {
                    List<com.netflix.titus.api.jobmanager.model.job.Job> allJobs = jobOperations.getJobs();
                    Set<String> knownJobIds = new HashSet<>();
                    allJobs.forEach(job -> knownJobIds.add(job.getId()));
                    return job -> !knownJobIds.contains(job.getId());
                },
                configuration,
                titusRuntime
        );

        this.taskCache = new ProtobufCache<>(
                "tasks",
                coreTask -> {
                    Task task = GrpcJobManagementModelConverters.toGrpcTask(coreTask, logStorageInfo, jobOperations.getEphemeralTaskStatus(coreTask.getId()));
                    // Need to serialize it once since GeneratedMessageV3 isn't entirely immutable.
                    // It needs to initialize memoizedSize field (non-volatile) before being used from multiple threads
                    serializeGrpcObject(task);
                    return task;
                },
                () -> {
                    List<com.netflix.titus.api.jobmanager.model.job.Task> allTasks = jobOperations.getTasks();
                    Set<String> knownTasksIds = new HashSet<>();
                    allTasks.forEach(task -> knownTasksIds.add(task.getId()));
                    return task -> !knownTasksIds.contains(task.getId());
                },
                configuration,
                titusRuntime
        );
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(jobCache, ProtobufCache::shutdown);
        Evaluators.acceptNotNull(taskCache, ProtobufCache::shutdown);
    }

    public Job getJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        if (configuration.isGrpcObjectsCacheEnabled()) {
            return jobCache.get(coreJob.getId(), coreJob);
        } else {
            return GrpcJobManagementModelConverters.toGrpcJob(coreJob);
        }
    }

    public Task getTask(com.netflix.titus.api.jobmanager.model.job.Task coreTask) {
        if (configuration.isGrpcObjectsCacheEnabled()) {
            return taskCache.get(coreTask.getId(), coreTask);
        } else {
            return GrpcJobManagementModelConverters.toGrpcTask(coreTask, logStorageInfo, jobOperations.getEphemeralTaskStatus(coreTask.getId()));
        }
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
