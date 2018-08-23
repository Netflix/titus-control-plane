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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.ProtobufCopy;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.master.endpoint.common.CellDecorator;
import com.netflix.titus.master.endpoint.grpc.GrpcEndpointConfiguration;
import com.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.GrpcTitusServiceGateway;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.runtime.connector.jobmanager.JobManagementClient.JOB_MINIMUM_FIELD_SET;
import static com.netflix.titus.runtime.connector.jobmanager.JobManagementClient.TASK_MINIMUM_FIELD_SET;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toJobQueryCriteria;
import static com.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toPage;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.endpoint.metadata.CallMetadataUtils.execute;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;

@Singleton
public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private final GrpcEndpointConfiguration configuration;
    private final AgentManagementService agentManagementService;
    private final ApplicationSlaManagementService capacityGroupService;
    private final TitusServiceGateway<String, JobDescriptor, JobSpecCase, Job, Task, TaskStatus.TaskState> serviceGateway;
    private final CallMetadataResolver callMetadataResolver;
    private final CellDecorator cellDecorator;

    @Inject
    public DefaultJobManagementServiceGrpc(GrpcEndpointConfiguration configuration,
                                           AgentManagementService agentManagementService,
                                           ApplicationSlaManagementService capacityGroupService,
                                           GrpcTitusServiceGateway serviceGateway,
                                           CallMetadataResolver callMetadataResolver,
                                           CellInfoResolver cellInfoResolver) {
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
        this.capacityGroupService = capacityGroupService;
        this.serviceGateway = serviceGateway;
        this.callMetadataResolver = callMetadataResolver;
        this.cellDecorator = new CellDecorator(cellInfoResolver::getCellName);
    }

    @Override
    public void createJob(JobDescriptor jobDescriptor, StreamObserver<JobId> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            if (configuration.isJobSizeValidationEnabled()) {
                com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);

                Tier tier = findTier(coreJobDescriptor);
                ResourceDimension requestedResources = toResourceDimension(coreJobDescriptor.getContainer().getContainerResources());
                List<ResourceDimension> tierResourceLimits = getTierResourceLimits(tier);
                if (isTooLarge(requestedResources, tierResourceLimits)) {
                    safeOnError(logger,
                            JobManagerException.invalidContainerResources(tier, requestedResources, tierResourceLimits),
                            responseObserver
                    );
                    return;
                }
            }

            JobDescriptor withCellInfo = cellDecorator.ensureCellInfo(jobDescriptor);

            serviceGateway.createJob(withCellInfo).subscribe(
                    jobId -> responseObserver.onNext(JobId.newBuilder().setId(jobId).build()),
                    e -> safeOnError(logger, e, responseObserver),
                    responseObserver::onCompleted
            );
        });
    }

    @Override
    public void findJobs(JobQuery jobQuery, StreamObserver<JobQueryResult> responseObserver) {
        if (!checkPageIsValid(jobQuery.getPage(), responseObserver)) {
            return;
        }
        try {
            JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> criteria = toJobQueryCriteria(jobQuery);
            Pair<List<Job>, Pagination> queryResult = serviceGateway.findJobsByCriteria(
                    criteria, Optional.of(toPage(jobQuery.getPage()))
            );

            if (!jobQuery.getFieldsList().isEmpty()) {
                Set<String> fields = new HashSet<>(jobQuery.getFieldsList());
                fields.addAll(JOB_MINIMUM_FIELD_SET);
                queryResult = queryResult.mapLeft(jobs -> jobs.stream().map(j -> ProtobufCopy.copy(j, fields)).collect(Collectors.toList()));
            }

            responseObserver.onNext(toJobQueryResult(queryResult.getLeft(), queryResult.getRight()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        String id = request.getId();

        serviceGateway.findJobById(id, true, Collections.emptySet()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
    }

    @Override
    public void findTasks(TaskQuery taskQuery, StreamObserver<TaskQueryResult> responseObserver) {
        if (!checkPageIsValid(taskQuery.getPage(), responseObserver)) {
            return;
        }
        try {
            JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> criteria = toJobQueryCriteria(taskQuery);
            Pair<List<Task>, Pagination> queryResult = serviceGateway.findTasksByCriteria(
                    criteria, Optional.of(toPage(taskQuery.getPage()))
            );

            if (!taskQuery.getFieldsList().isEmpty()) {
                Set<String> fields = new HashSet<>(taskQuery.getFieldsList());
                fields.addAll(TASK_MINIMUM_FIELD_SET);
                queryResult = queryResult.mapLeft(tasks -> tasks.stream().map(t -> ProtobufCopy.copy(t, fields)).collect(Collectors.toList()));
            }

            responseObserver.onNext(toTaskQueryResult(queryResult.getLeft(), queryResult.getRight()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void findTask(TaskId request, StreamObserver<Task> responseObserver) {
        String id = request.getId();

        serviceGateway.findTaskById(id).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
    }

    @Override
    public void updateJobCapacity(JobCapacityUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            Capacity taskInstances = request.getCapacity();
            serviceGateway.resizeJob(
                    toReasonString(callMetadata), request.getJobId(), taskInstances.getDesired(), taskInstances.getMin(), taskInstances.getMax()
            ).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void updateJobProcesses(JobProcessesUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata -> {
            ServiceJobSpec.ServiceJobProcesses serviceJobProcesses = request.getServiceJobProcesses();
            serviceGateway.updateJobProcesses(
                    toReasonString(callMetadata), request.getJobId(), serviceJobProcesses.getDisableDecreaseDesired(), serviceJobProcesses.getDisableIncreaseDesired()
            ).subscribe(
                    nothing -> {
                    },
                    e -> safeOnError(logger, e, responseObserver),
                    () -> {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
            );
        });
    }

    @Override
    public void updateJobStatus(JobStatusUpdate request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                serviceGateway.changeJobInServiceStatus(
                        toReasonString(callMetadata), request.getId(), request.getEnableStatus()
                ).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void killJob(JobId request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                serviceGateway.killJob(
                        toReasonString(callMetadata), request.getId()
                ).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void killTask(TaskKillRequest request, StreamObserver<Empty> responseObserver) {
        execute(callMetadataResolver, responseObserver, callMetadata ->
                serviceGateway.killTask(
                        toReasonString(callMetadata), request.getTaskId(), request.getShrink()
                ).subscribe(
                        nothing -> {
                        },
                        e -> safeOnError(logger, e, responseObserver),
                        () -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                        }
                ));
    }

    @Override
    public void observeJobs(Empty request, StreamObserver<JobChangeNotification> responseObserver) {
        Observable<JobChangeNotification> eventStream = serviceGateway.observeJobs();
        Subscription subscription = eventStream.subscribe(
                responseObserver::onNext,
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription("All jobs monitoring stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        String id = request.getId();
        Observable<JobChangeNotification> eventStream = serviceGateway.observeJob(id);
        Subscription subscription = eventStream.subscribe(
                responseObserver::onNext,
                e -> responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL
                                .withDescription(id + " job monitoring stream terminated with an error")
                                .withCause(e))
                ),
                responseObserver::onCompleted
        );

        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(subscription::unsubscribe);
    }

    private JobQueryResult toJobQueryResult(List<Job> jobs, Pagination runtimePagination) {
        return JobQueryResult.newBuilder()
                .addAllItems(jobs)
                .setPagination(toGrpcPagination(runtimePagination))
                .build();
    }

    private TaskQueryResult toTaskQueryResult(List<Task> tasks, Pagination runtimePagination) {
        return TaskQueryResult.newBuilder()
                .addAllItems(tasks)
                .setPagination(toGrpcPagination(runtimePagination))
                .build();
    }

    private ResourceDimension toResourceDimension(ContainerResources containerResources) {
        return ResourceDimension.newBuilder()
                .withCpus(containerResources.getCpu())
                .withGpu(containerResources.getGpu())
                .withMemoryMB(containerResources.getMemoryMB())
                .withDiskMB(containerResources.getDiskMB())
                .withNetworkMbs(containerResources.getNetworkMbps())
                .build();
    }

    private String toReasonString(CallMetadata callMetadata) {
        StringBuilder builder = new StringBuilder();
        builder.append("calledBy=").append(callMetadata.getCallerId());
        builder.append(", relayedVia=");

        List<String> callPath = callMetadata.getCallPath();
        if (callPath.isEmpty()) {
            builder.append("direct to TitusMaster");
        } else {
            for (int i = 0; i < callPath.size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append(callPath.get(i));
            }
        }

        if (StringExt.isNotEmpty(callMetadata.getCallReason())) {
            builder.append(", reason=").append(callMetadata.getCallReason());
        }

        return builder.toString();
    }

    private Tier findTier(com.netflix.titus.api.jobmanager.model.job.JobDescriptor jobDescriptor) {
        return JobManagerUtil.getTierAssignment(jobDescriptor, capacityGroupService).getLeft();
    }

    private List<ResourceDimension> getTierResourceLimits(Tier tier) {
        return agentManagementService.getInstanceGroups().stream()
                .filter(instanceGroup -> instanceGroup.getTier().equals(tier))
                .map(instanceGroup ->
                        agentManagementService.findResourceLimits(instanceGroup.getInstanceType()).orElse(instanceGroup.getResourceDimension())
                )
                .collect(Collectors.toList());
    }

    private boolean isTooLarge(ResourceDimension requestedResources, List<ResourceDimension> tierResourceLimits) {
        return tierResourceLimits.stream().noneMatch(limit -> ResourceDimensions.isBigger(limit, requestedResources));
    }
}
