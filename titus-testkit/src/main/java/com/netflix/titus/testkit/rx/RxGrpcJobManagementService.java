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

package com.netflix.titus.testkit.rx;

import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import rx.Observable;

/**
 * Rx wrapper for {@link com.netflix.titus.grpc.protogen.JobManagementServiceGrpc}.
 */
public class RxGrpcJobManagementService {
    private final ManagedChannel channel;

    public RxGrpcJobManagementService(ManagedChannel channel) {
        this.channel = channel;
    }

    public Observable<JobId> createJob(JobDescriptor jobDescriptor) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_CREATE_JOB, CallOptions.DEFAULT),
                    jobDescriptor
            ).subscribe(subscriber);
        });
    }

    public Observable<JobQueryResult> findJobs(JobQuery query) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_FIND_JOBS, CallOptions.DEFAULT),
                    query
            ).subscribe(subscriber);
        });
    }

    public Observable<Job> findJob(JobId jobId) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_FIND_JOB, CallOptions.DEFAULT),
                    jobId
            ).subscribe(subscriber);
        });
    }

    public Observable<TaskQueryResult> findTasks(TaskQuery query) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_FIND_TASKS, CallOptions.DEFAULT),
                    query
            ).subscribe(subscriber);
        });
    }

    public Observable<Task> findTask(TaskId taskId) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_FIND_TASK, CallOptions.DEFAULT),
                    taskId
            ).subscribe(subscriber);
        });
    }

    public Observable<Void> updateInServiceStatus(String jobId, boolean inService) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_UPDATE_JOB_STATUS, CallOptions.DEFAULT),
                    JobStatusUpdate.newBuilder().setId(jobId).setEnableStatus(inService).build()
            ).ignoreElements().cast(Void.class).subscribe(subscriber);
        });
    }

    public Observable<Void> updateJobSize(String jobId, int min, int desired, int max) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_UPDATE_JOB_CAPACITY, CallOptions.DEFAULT),
                    JobCapacityUpdate.newBuilder()
                            .setJobId(jobId)
                            .setCapacity(Capacity.newBuilder().setMin(min).setDesired(desired).setMax(max))
                            .build()
            ).ignoreElements().cast(Void.class).subscribe(subscriber);
        });
    }

    public Observable<Void> updateJobProcesses(String jobId, boolean disableIncrease, boolean disableDecrease) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_UPDATE_JOB_PROCESSES, CallOptions.DEFAULT),
                    JobProcessesUpdate.newBuilder()
                            .setJobId(jobId)
                            .setServiceJobProcesses(ServiceJobSpec.ServiceJobProcesses.newBuilder()
                                    .setDisableIncreaseDesired(disableIncrease)
                                    .setDisableDecreaseDesired(disableDecrease)
                                    .build())
                            .build()
            ).ignoreElements().cast(Void.class).subscribe(subscriber);
        });
    }

    public Observable<Void> killJob(String jobId) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_KILL_JOB, CallOptions.DEFAULT),
                    JobId.newBuilder().setId(jobId).build()
            ).ignoreElements().cast(Void.class).subscribe(subscriber);
        });
    }

    public Observable<Void> killTask(String taskId, boolean shrink) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_KILL_TASK, CallOptions.DEFAULT),
                    TaskKillRequest.newBuilder().setTaskId(taskId).setShrink(shrink).build()
            ).ignoreElements().cast(Void.class).subscribe(subscriber);
        });
    }

    public Observable<JobChangeNotification> observeJobs() {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_OBSERVE_JOBS, CallOptions.DEFAULT),
                    ObserveJobsQuery.newBuilder().build()
            ).subscribe(subscriber);
        });
    }

    public Observable<JobChangeNotification> observeJob(JobId jobId) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(JobManagementServiceGrpc.METHOD_OBSERVE_JOB, CallOptions.DEFAULT),
                    jobId
            ).subscribe(subscriber);
        });
    }
}
