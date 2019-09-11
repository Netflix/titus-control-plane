/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactorJobManagementServiceStub {

    // ------------------------------------------------------------
    // Job operations

    Mono<JobId> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata);

    Mono<Void> updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata);

    Mono<Void> updateJobStatus(JobStatusUpdate jobStatusUpdate, CallMetadata callMetadata);

    Mono<Void> updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata);

    Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate jobDisruptionBudgetUpdate, CallMetadata callMetadata);

    Mono<JobQueryResult> findJobs(JobQuery jobQuery);

    Mono<Job> findJob(JobId jobId);

    Flux<JobChangeNotification> observeJob(JobId jobId);

    Flux<JobChangeNotification> observeJobs(ObserveJobsQuery observeJobsQuery);

    Mono<Void> killJob(JobId jobId, CallMetadata callMetadata);

    Mono<Void> updateJobAttributes(JobAttributesUpdate jobAttributesUpdate, CallMetadata callMetadata);

    Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest jobAttributesDeleteRequest, CallMetadata callMetadata);

    // ------------------------------------------------------------
    // Task operations

    Mono<Task> findTask(TaskId taskId);

    Mono<TaskQueryResult> findTasks(TaskQuery taskQuery);

    Mono<Void> killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata);

    Mono<Void> updateTaskAttributes(TaskAttributesUpdate taskAttributesUpdate, CallMetadata callMetadata);

    Mono<Void> deleteTaskAttributes(TaskAttributesDeleteRequest taskAttributesDeleteRequest, CallMetadata callMetadata);

    Mono<Void> moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata);
}
