/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcObjectsCache;
import rx.Scheduler;

/**
 * A helper class to deal with job event stream subscriptions.
 */
class ObserveJobsContext {

    static final JobChangeNotification SNAPSHOT_END_MARKER = JobChangeNotification.newBuilder()
            .setSnapshotEnd(JobChangeNotification.SnapshotEnd.newBuilder())
            .build();

    private final V3JobOperations jobOperations;
    private final CallMetadataResolver callMetadataResolver;
    private final GrpcObjectsCache grpcObjectsCache;
    private final Scheduler observeJobsScheduler;
    private final DefaultJobManagementServiceGrpcMetrics metrics;
    private final TitusRuntime titusRuntime;

    ObserveJobsContext(V3JobOperations jobOperations,
                       CallMetadataResolver callMetadataResolver,
                       GrpcObjectsCache grpcObjectsCache,
                       Scheduler observeJobsScheduler,
                       DefaultJobManagementServiceGrpcMetrics metrics,
                       TitusRuntime titusRuntime) {
        this.jobOperations = jobOperations;
        this.callMetadataResolver = callMetadataResolver;
        this.grpcObjectsCache = grpcObjectsCache;
        this.observeJobsScheduler = observeJobsScheduler;
        this.metrics = metrics;
        this.titusRuntime = titusRuntime;
    }

    public V3JobOperations getJobOperations() {
        return jobOperations;
    }

    public CallMetadataResolver getCallMetadataResolver() {
        return callMetadataResolver;
    }

    public GrpcObjectsCache getGrpcObjectsCache() {
        return grpcObjectsCache;
    }

    public Scheduler getObserveJobsScheduler() {
        return observeJobsScheduler;
    }

    public DefaultJobManagementServiceGrpcMetrics getMetrics() {
        return metrics;
    }

    public TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }


    JobChangeNotification toJobChangeNotification(com.netflix.titus.api.jobmanager.model.job.Job<?> coreJob, long now) {
        com.netflix.titus.grpc.protogen.Job grpcJob = grpcObjectsCache.getJob(coreJob);
        return JobChangeNotification.newBuilder()
                .setJobUpdate(JobChangeNotification.JobUpdate.newBuilder().setJob(grpcJob))
                .setTimestamp(now)
                .build();
    }

    JobChangeNotification toJobChangeNotification(com.netflix.titus.api.jobmanager.model.job.Task coreTask, long now) {
        com.netflix.titus.grpc.protogen.Task grpcTask = grpcObjectsCache.getTask(coreTask);
        return JobChangeNotification.newBuilder()
                .setTaskUpdate(JobChangeNotification.TaskUpdate.newBuilder().setTask(grpcTask))
                .setTimestamp(now)
                .build();
    }
}
