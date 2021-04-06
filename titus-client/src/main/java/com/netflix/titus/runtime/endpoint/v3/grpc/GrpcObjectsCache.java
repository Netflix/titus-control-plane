package com.netflix.titus.runtime.endpoint.v3.grpc;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;

public interface GrpcObjectsCache {
    /*
     Get Grpc Job object for the corresponding core Job object
     */
    Job getJob(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob);

    /*
     Get Grpc Task object for the corresponding core Task object
     */
    Task getTask(com.netflix.titus.api.jobmanager.model.job.Task coreTask);
}
