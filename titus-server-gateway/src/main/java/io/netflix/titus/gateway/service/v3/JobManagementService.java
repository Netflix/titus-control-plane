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

package io.netflix.titus.gateway.service.v3;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import rx.Completable;
import rx.Observable;

public interface JobManagementService {
    Observable<String> createJob(JobDescriptor jobDescriptor);

    Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate);

    Completable changeJobInServiceStatus(JobStatusUpdate statusUpdate);

    Observable<Job> findJob(String jobId);

    Observable<JobQueryResult> findJobs(JobQuery jobQuery);

    Observable<JobChangeNotification> observeJob(String jobId);

    Observable<JobChangeNotification> observeJobs();

    Completable killJob(String jobId);

    Observable<Task> findTask(String taskId);

    Observable<TaskQueryResult> findTasks(TaskQuery taskQuery);

    Completable killTask(TaskKillRequest taskKillRequest);
}
