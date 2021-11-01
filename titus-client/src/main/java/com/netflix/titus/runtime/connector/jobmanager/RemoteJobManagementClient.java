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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class RemoteJobManagementClient implements JobManagementClient {

    private final String clientName;
    private final ReactorJobManagementServiceStub stub;
    private final TitusRuntime titusRuntime;

    @Inject
    public RemoteJobManagementClient(String clientName, ReactorJobManagementServiceStub stub, TitusRuntime titusRuntime) {
        this.clientName = clientName;
        this.stub = stub;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public Mono<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        return stub.createJob(GrpcJobManagementModelConverters.toGrpcJobDescriptor(jobDescriptor), callMetadata).map(JobId::getId);
    }

    @Override
    public Mono<Void> updateJobCapacity(String jobId, Capacity capacity, CallMetadata callMetadata) {
        return stub.updateJobCapacity(
                JobCapacityUpdate.newBuilder()
                        .setJobId(jobId)
                        .setCapacity(GrpcJobManagementModelConverters.toGrpcCapacity(capacity))
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Void> updateJobStatus(String jobId, boolean enabled, CallMetadata callMetadata) {
        return stub.updateJobStatus(
                JobStatusUpdate.newBuilder()
                        .setId(jobId)
                        .setEnableStatus(enabled)
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Void> updateJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata) {
        return stub.updateJobProcesses(
                JobProcessesUpdate.newBuilder()
                        .setJobId(jobId)
                        .setServiceJobProcesses(GrpcJobManagementModelConverters.toGrpcServiceJobProcesses(serviceJobProcesses))
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget, CallMetadata callMetadata) {
        return stub.updateJobDisruptionBudget(
                JobDisruptionBudgetUpdate.newBuilder()
                        .setJobId(jobId)
                        .setDisruptionBudget(GrpcJobManagementModelConverters.toGrpcDisruptionBudget(disruptionBudget))
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Job> findJob(String jobId) {
        return stub.findJob(JobId.newBuilder().setId(jobId).build()).map(GrpcJobManagementModelConverters::toCoreJob);
    }

    @Override
    public Mono<PageResult<Job<?>>> findJobs(Map<String, String> filteringCriteria, Page page) {
        return (Mono) stub.findJobs(
                JobQuery.newBuilder()
                        .setPage(GrpcJobQueryModelConverters.toGrpcPage(page))
                        .putAllFilteringCriteria(filteringCriteria)
                        .build()
        ).map(response -> PageResult.pageOf(
                response.getItemsList().stream().map(GrpcJobManagementModelConverters::toCoreJob).collect(Collectors.toList()),
                GrpcJobQueryModelConverters.toPagination(response.getPagination())
        ));
    }

    /**
     * Single job subscription has a limitation that for tasks moved to another job there will be no notification.
     */
    @Override
    public Flux<JobManagerEvent<?>> observeJob(String jobId) {
        return Flux.defer(() -> {
            AtomicReference<Job> jobRef = new AtomicReference<>();
            Map<String, Task> taskMap = new ConcurrentHashMap<>();
            return stub.observeJob(JobId.newBuilder().setId(jobId).build())
                    .map(event -> {
                        switch (event.getNotificationCase()) {
                            case JOBUPDATE:
                                Job newJob = GrpcJobManagementModelConverters.toCoreJob(event.getJobUpdate().getJob());
                                Job oldJob = jobRef.get();
                                jobRef.set(newJob);
                                return oldJob == null
                                        ? JobUpdateEvent.newJob(newJob, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA)
                                        : JobUpdateEvent.jobChange(newJob, oldJob, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
                            case TASKUPDATE:
                                Task newTask = GrpcJobManagementModelConverters.toCoreTask(jobRef.get(), event.getTaskUpdate().getTask());
                                Task oldTask = taskMap.get(newTask.getId());
                                taskMap.put(newTask.getId(), newTask);

                                if (event.getTaskUpdate().getMovedFromAnotherJob()) {
                                    return TaskUpdateEvent.newTaskFromAnotherJob(jobRef.get(), newTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
                                }

                                return oldTask == null
                                        ? TaskUpdateEvent.newTask(jobRef.get(), newTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA)
                                        : TaskUpdateEvent.taskChange(jobRef.get(), newTask, oldTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
                            case SNAPSHOTEND:
                                return JobManagerEvent.snapshotMarker();
                            case KEEPALIVERESPONSE:
                                return JobManagerEvent.keepAliveEvent(event.getKeepAliveResponse().getRequest().getTimestamp());
                            case NOTIFICATION_NOT_SET:
                            default:
                                return null;
                        }
                    })
                    .filter(Objects::nonNull);
        });
    }

    @Override
    public Flux<JobManagerEvent<?>> observeJobs(Map<String, String> filteringCriteria) {
        return Flux.defer(() -> {
            Map<String, Job> jobMap = new ConcurrentHashMap<>();
            Map<String, Task> taskMap = new ConcurrentHashMap<>();
            return connectObserveJobs(filteringCriteria)
                    .map(event -> {
                        switch (event.getNotificationCase()) {
                            case JOBUPDATE:
                                Job newJob = JobEventPropagationUtil.recordChannelLatency(
                                        clientName,
                                        GrpcJobManagementModelConverters.toCoreJob(event.getJobUpdate().getJob()),
                                        event.getTimestamp(),
                                        titusRuntime.getClock()
                                );

                                Job oldJob = jobMap.get(newJob.getId());
                                jobMap.put(newJob.getId(), newJob);
                                return oldJob == null
                                        ? JobUpdateEvent.newJob(newJob, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA)
                                        : JobUpdateEvent.jobChange(newJob, oldJob, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
                            case TASKUPDATE:
                                com.netflix.titus.grpc.protogen.Task grpcTask = event.getTaskUpdate().getTask();
                                Job job = jobMap.get(grpcTask.getJobId());

                                Task newTask = JobEventPropagationUtil.recordChannelLatency(
                                        clientName,
                                        GrpcJobManagementModelConverters.toCoreTask(job, grpcTask),
                                        event.getTimestamp(),
                                        titusRuntime.getClock()
                                );
                                Task oldTask = taskMap.get(newTask.getId());
                                taskMap.put(newTask.getId(), newTask);

                                // Check if task moved
                                if (isTaskMoved(newTask, oldTask)) {
                                    return TaskUpdateEvent.newTaskFromAnotherJob(job, newTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
                                }

                                return oldTask == null
                                        ? TaskUpdateEvent.newTask(job, newTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA)
                                        : TaskUpdateEvent.taskChange(job, newTask, oldTask, JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA);
                            case SNAPSHOTEND:
                                return JobManagerEvent.snapshotMarker();
                            case KEEPALIVERESPONSE:
                                return JobManagerEvent.keepAliveEvent(event.getKeepAliveResponse().getRequest().getTimestamp());
                            case NOTIFICATION_NOT_SET:
                            default:
                                return null;
                        }
                    })
                    .filter(Objects::nonNull);
        });
    }

    protected Flux<JobChangeNotification> connectObserveJobs(Map<String, String> filteringCriteria) {
        return stub.observeJobs(ObserveJobsQuery.newBuilder().putAllFilteringCriteria(filteringCriteria).build());
    }

    @Override
    public Mono<Void> killJob(String jobId, CallMetadata callMetadata) {
        return stub.killJob(JobId.newBuilder().setId(jobId).build(), callMetadata);
    }

    @Override
    public Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes, CallMetadata callMetadata) {
        return stub.updateJobAttributes(
                JobAttributesUpdate.newBuilder()
                        .setJobId(jobId)
                        .putAllAttributes(attributes)
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Void> deleteJobAttributes(String jobId, Set<String> attributeKeys, CallMetadata callMetadata) {
        return stub.deleteJobAttributes(
                JobAttributesDeleteRequest.newBuilder()
                        .setJobId(jobId)
                        .addAllKeys(attributeKeys)
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Task> findTask(String taskId) {
        return stub.findTask(TaskId.newBuilder().setId(taskId).build()).map(GrpcJobManagementModelConverters::toCoreTask);
    }

    @Override
    public Mono<PageResult<Task>> findTasks(Map<String, String> filteringCriteria, Page page) {
        return stub.findTasks(
                TaskQuery.newBuilder()
                        .setPage(GrpcJobQueryModelConverters.toGrpcPage(page))
                        .putAllFilteringCriteria(filteringCriteria)
                        .build()
        ).map(response -> PageResult.pageOf(
                response.getItemsList().stream().map(GrpcJobManagementModelConverters::toCoreTask).collect(Collectors.toList()),
                GrpcJobQueryModelConverters.toPagination(response.getPagination())
        ));
    }

    @Override
    public Mono<Void> killTask(String taskId, boolean shrink, CallMetadata callMetadata) {
        return stub.killTask(TaskKillRequest.newBuilder().setTaskId(taskId).setShrink(shrink).build(), callMetadata);
    }

    @Override
    public Mono<Void> updateTaskAttributes(String taskId, Map<String, String> attributes, CallMetadata callMetadata) {
        return stub.updateTaskAttributes(
                TaskAttributesUpdate.newBuilder()
                        .setTaskId(taskId)
                        .putAllAttributes(attributes)
                        .build(),
                callMetadata
        );
    }

    @Override
    public Mono<Void> deleteTaskAttributes(String taskId, Set<String> attributeKeys, CallMetadata callMetadata) {
        return stub.deleteTaskAttributes(
                TaskAttributesDeleteRequest.newBuilder().setTaskId(taskId).addAllKeys(attributeKeys).build(),
                callMetadata
        );
    }

    @Override
    public Mono<Void> moveTask(String sourceJobId, String targetJobId, String taskId, CallMetadata callMetadata) {
        return stub.moveTask(
                TaskMoveRequest.newBuilder()
                        .setSourceJobId(sourceJobId)
                        .setTargetJobId(targetJobId)
                        .setTaskId(taskId)
                        .build(),
                callMetadata
        );
    }

    static boolean isTaskMoved(Task newTask, Task oldTask) {
        if (oldTask == null || oldTask.getJobId().equals(newTask.getJobId())) {
            return false;
        }
        return newTask.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_MOVED_FROM_JOB) != null;
    }
}
