package com.netflix.titus.testkit.model.job;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcObjectsCache;

public class NoOpGrpcObjectsCache implements GrpcObjectsCache {
    @Override
    public Job getJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob) {
        return GrpcJobManagementModelConverters.toGrpcJob(coreJob);
    }

    @Override
    public Task getTask(com.netflix.titus.api.jobmanager.model.job.Task coreTask) {
        return GrpcJobManagementModelConverters.toGrpcTask(coreTask, EmptyLogStorageInfo.empty());
    }
}
