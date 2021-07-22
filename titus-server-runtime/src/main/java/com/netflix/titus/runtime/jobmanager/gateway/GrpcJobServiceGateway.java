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

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.JobAttributesUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDisruptionBudgetUpdate;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.TaskAttributesDeleteRequest;
import com.netflix.titus.grpc.protogen.TaskAttributesUpdate;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskMoveRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.JobEventPropagationUtil;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.connector.jobmanager.JobEventPropagationUtil.CHECKPOINT_GATEWAY_CLIENT;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createMonoVoidRequest;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

/**
 * {@link JobServiceGateway} implementation that connects to TitusMaster over the GRPC channel.
 */
@Singleton
public class GrpcJobServiceGateway implements JobServiceGateway {

    private final JobManagementServiceGrpc.JobManagementServiceStub client;
    private final GrpcRequestConfiguration configuration;
    private final TitusRuntime titusRuntime;

    @Inject
    public GrpcJobServiceGateway(JobManagementServiceGrpc.JobManagementServiceStub client,
                                 GrpcRequestConfiguration configuration,
                                 TitusRuntime titusRuntime) {
        this.client = client;
        this.configuration = configuration;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            V3HeaderInterceptor.attachCallMetadata(client, callMetadata)
                    .withDeadlineAfter(configuration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS)
                    .createJob(jobDescriptor, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateJobCapacity(jobCapacityUpdate, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes,
                                                               CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateJobProcesses(jobProcessesUpdate, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateJobStatus(statusUpdate, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        return createMonoVoidRequest(
                emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientMonoResponse(emitter);
                    createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateJobDisruptionBudget(request, streamObserver);
                },
                configuration.getRequestTimeoutMs()
        ).ignoreElement().cast(Void.class);
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        return createMonoVoidRequest(
                emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientMonoResponse(emitter);
                    createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateJobAttributes(request, streamObserver);
                },
                configuration.getRequestTimeoutMs()
        ).ignoreElement().cast(Void.class);
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        return createMonoVoidRequest(
                emitter -> {
                    StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientMonoResponse(emitter);
                    createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).deleteJobAttributes(request, streamObserver);
                },
                configuration.getRequestTimeoutMs()
        ).ignoreElement().cast(Void.class);
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        Observable<Job> observable = createRequestObservable(emitter -> {
            StreamObserver<Job> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).findJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, configuration.getRequestTimeoutMs());
        return observable.timeout(configuration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).findJobs(jobQuery, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata).observeJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        Observable<JobChangeNotification> source = createRequestObservable(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata).observeJobs(query, streamObserver);
        });
        return source.map(event -> {
            if (event.getNotificationCase() == JobChangeNotification.NotificationCase.JOBUPDATE) {
                return event.toBuilder().setJobUpdate(
                        event.getJobUpdate().toBuilder()
                                .setJob(JobEventPropagationUtil.recordChannelLatency(
                                        CHECKPOINT_GATEWAY_CLIENT,
                                        event.getJobUpdate().getJob(),
                                        event.getTimestamp(),
                                        titusRuntime.getClock()
                                ))
                                .build()
                ).build();
            }
            if (event.getNotificationCase() == JobChangeNotification.NotificationCase.TASKUPDATE) {
                return event.toBuilder().setTaskUpdate(
                        event.getTaskUpdate().toBuilder()
                                .setTask(JobEventPropagationUtil.recordChannelLatency(
                                        CHECKPOINT_GATEWAY_CLIENT,
                                        event.getTaskUpdate().getTask(),
                                        event.getTimestamp(),
                                        titusRuntime.getClock()
                                ))
                                .build()
                ).build();
            }
            return event;
        });
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).killJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Observable<com.netflix.titus.grpc.protogen.Task> findTask(String taskId, CallMetadata callMetadata) {
        Observable<com.netflix.titus.grpc.protogen.Task> observable = createRequestObservable(emitter -> {
            StreamObserver<com.netflix.titus.grpc.protogen.Task> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).findTask(TaskId.newBuilder().setId(taskId).build(), streamObserver);
        }, configuration.getRequestTimeoutMs());
        return observable.timeout(configuration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        Observable<TaskQueryResult> observable = createRequestObservable(emitter -> {
            StreamObserver<TaskQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).findTasks(taskQuery, streamObserver);
        }, configuration.getRequestTimeoutMs());
        return observable.timeout(configuration.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).killTask(taskKillRequest, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate attributesUpdate, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).updateTaskAttributes(attributesUpdate, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest deleteRequest, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).deleteTaskAttributes(deleteRequest, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadata, configuration.getRequestTimeoutMs()).moveTask(taskMoveRequest, streamObserver);
        }, configuration.getRequestTimeoutMs());
    }
}
