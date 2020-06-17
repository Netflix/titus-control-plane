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

public class JobServiceGatewayDelegate implements JobServiceGateway {

    private final JobServiceGateway delegate;

    public JobServiceGatewayDelegate(JobServiceGateway delegate) {
        this.delegate = delegate;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        return delegate.createJob(jobDescriptor, callMetadata);
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        return delegate.updateJobCapacity(jobCapacityUpdate, callMetadata);
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes,
                                                               CallMetadata callMetadata) {
        return delegate.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, callMetadata);
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        return delegate.updateJobProcesses(jobProcessesUpdate, callMetadata);
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        return delegate.updateJobStatus(statusUpdate, callMetadata);
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        return delegate.updateJobDisruptionBudget(request, callMetadata);
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        return delegate.updateJobAttributes(request, callMetadata);
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        return delegate.deleteJobAttributes(request, callMetadata);
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        return delegate.findJob(jobId, callMetadata);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        return delegate.findJobs(jobQuery, callMetadata);
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        return delegate.observeJob(jobId, callMetadata);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        return delegate.observeJobs(query, callMetadata);
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        return delegate.killJob(jobId, callMetadata);
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        return delegate.findTask(taskId, callMetadata);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        return delegate.findTasks(taskQuery, callMetadata);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        return delegate.killTask(taskKillRequest, callMetadata);
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate attributesUpdate, CallMetadata callMetadata) {
        return delegate.updateTaskAttributes(attributesUpdate, callMetadata);
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest deleteRequest, CallMetadata callMetadata) {
        return delegate.deleteTaskAttributes(deleteRequest, callMetadata);
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        return delegate.moveTask(taskMoveRequest, callMetadata);
    }
}
