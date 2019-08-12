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

package com.netflix.titus.runtime.jobmanager.gateway;

import java.util.Set;

import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

/**
 * Remote job management client API.
 */
public interface JobServiceGateway {

    Set<String> JOB_MINIMUM_FIELD_SET = asSet("id");

    Set<String> TASK_MINIMUM_FIELD_SET = asSet("id");

    Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata);

    Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate);

    Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes);

    Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate);

    Completable updateJobStatus(JobStatusUpdate statusUpdate);

    Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request);

    Mono<Void> updateJobAttributes(JobAttributesUpdate request);

    Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request);

    Observable<Job> findJob(String jobId);

    Observable<JobQueryResult> findJobs(JobQuery jobQuery);

    Observable<JobChangeNotification> observeJob(String jobId);

    Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query);

    Completable killJob(String jobId);

    Observable<Task> findTask(String taskId);

    Observable<TaskQueryResult> findTasks(TaskQuery taskQuery);

    Completable killTask(TaskKillRequest taskKillRequest);

    Completable updateTaskAttributes(TaskAttributesUpdate request);

    Completable deleteTaskAttributes(TaskAttributesDeleteRequest request);

    Completable moveTask(TaskMoveRequest taskMoveRequest);
}
