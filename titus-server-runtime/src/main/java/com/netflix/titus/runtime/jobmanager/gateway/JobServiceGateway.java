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

import com.netflix.titus.api.model.callmetadata.CallMetadata;
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

    Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata);

    Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes,
                                                        CallMetadata callMetadata);

    Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata);

    Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata);

    Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata);

    Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata);

    Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata);

    Observable<Job> findJob(String jobId, CallMetadata callMetadata);

    Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata);

    Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata);

    Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata);

    Completable killJob(String jobId, CallMetadata callMetadata);

    Observable<Task> findTask(String taskId, CallMetadata callMetadata);

    Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata);

    Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata);

    Completable updateTaskAttributes(TaskAttributesUpdate request, CallMetadata callMetadata);

    Completable deleteTaskAttributes(TaskAttributesDeleteRequest request, CallMetadata callMetadata);

    Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata);
}
