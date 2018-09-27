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

package com.netflix.titus.runtime.connector.jobmanager.client;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.util.rx.ReactorExt;
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
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import com.netflix.titus.runtime.connector.GrpcClientConfiguration;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

/**
 * {@link JobManagementClient} implementation that connects to TitusMaster over the GRPC channel.
 */
@Singleton
public class GrpcJobManagementClient implements JobManagementClient {

    private static final Logger logger = LoggerFactory.getLogger(GrpcJobManagementClient.class);

    private final JobManagementServiceGrpc.JobManagementServiceStub client;
    private final CallMetadataResolver callMetadataResolver;
    private final EntitySanitizer entitySanitizer;
    private final GrpcClientConfiguration configuration;
    private final EntityValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validator;
    private final Registry registry;

    @Inject
    public GrpcJobManagementClient(JobManagementServiceGrpc.JobManagementServiceStub client,
                                   CallMetadataResolver callMetadataResolver,
                                   @Named(JOB_STRICT_SANITIZER) EntitySanitizer entitySanitizer,
                                   EntityValidator<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> validator,
                                   GrpcClientConfiguration configuration,
                                   Registry registry) {
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
        this.entitySanitizer = entitySanitizer;
        this.validator = validator;
        this.configuration = configuration;
        this.registry = registry;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor coreJobDescriptor;
        try {
            coreJobDescriptor = V3GrpcModelConverters.toCoreJobDescriptor(jobDescriptor);
        } catch (Exception e) {
            return Observable.error(TitusServiceException.invalidArgument(e));
        }
        com.netflix.titus.api.jobmanager.model.job.JobDescriptor sanitizedCoreJobDescriptor = entitySanitizer.sanitize(coreJobDescriptor).orElse(coreJobDescriptor);

        Set<ValidationError> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }

        Observable<com.netflix.titus.api.jobmanager.model.job.JobDescriptor> sanitizedCoreJobDescriptorObs =
                ReactorExt.toObservable(validator.sanitize(sanitizedCoreJobDescriptor))
                        .onErrorResumeNext(throwable -> Observable.error(TitusServiceException.invalidArgument(throwable)))
                        .flatMap(scjd -> ReactorExt.toObservable(validator.validate(scjd))
                                .flatMap(errors -> {
                                    // Report metrics on all errors
                                    reportErrorMetrics(errors);

                                    // Only emit an error on HARD validation errors
                                    errors = errors.stream().filter(error -> error.isHard()).collect(Collectors.toSet());

                                    if (!errors.isEmpty()) {
                                        return Observable.error(TitusServiceException.invalidJob(errors));
                                    } else {
                                        return Observable.just(scjd);
                                    }
                                })
                        );

        return sanitizedCoreJobDescriptorObs
                .flatMap(scjd -> {
                    JobDescriptor effectiveJobDescriptor = V3GrpcModelConverters.toGrpcJobDescriptor(scjd);
                    return createRequestObservable(emitter -> {
                        StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                                emitter,
                                jobId -> emitter.onNext(jobId.getId()),
                                emitter::onError,
                                emitter::onCompleted
                        );
                        createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).createJob(effectiveJobDescriptor, streamObserver);
                    }, configuration.getRequestTimeout());
                });
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        Capacity newCapacity = V3GrpcModelConverters.toCoreCapacity(jobCapacityUpdate.getCapacity());
        Set<ValidationError> violations = entitySanitizer.validate(newCapacity);
        if (!violations.isEmpty()) {
            return Completable.error(TitusServiceException.invalidArgument(violations));
        }
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateJobCapacity(jobCapacityUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateJobProcesses(jobProcessesUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).updateJobStatus(statusUpdate, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        Observable<Job> observable = createRequestObservable(emitter -> {
            StreamObserver<Job> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, configuration.getRequestTimeout());
        return observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findJobs(jobQuery, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return createRequestObservable(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        });
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return createRequestObservable(emitter -> {
            StreamObserver<JobChangeNotification> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver).observeJobs(Empty.getDefaultInstance(), streamObserver);
        });
    }

    @Override
    public Completable killJob(String jobId) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).killJob(JobId.newBuilder().setId(jobId).build(), streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Observable<com.netflix.titus.grpc.protogen.Task> findTask(String taskId) {
        Observable<com.netflix.titus.grpc.protogen.Task> observable = createRequestObservable(emitter -> {
            StreamObserver<com.netflix.titus.grpc.protogen.Task> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findTask(TaskId.newBuilder().setId(taskId).build(), streamObserver);
        }, configuration.getRequestTimeout());
        return observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery) {
        Observable<TaskQueryResult> observable = createRequestObservable(emitter -> {
            StreamObserver<TaskQueryResult> streamObserver = createSimpleClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).findTasks(taskQuery, streamObserver);
        }, configuration.getRequestTimeout());
        return observable.timeout(configuration.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest) {
        return createRequestCompletable(emitter -> {
            StreamObserver<Empty> streamObserver = GrpcUtil.createEmptyClientResponseObserver(emitter);
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).killTask(taskKillRequest, streamObserver);
        }, configuration.getRequestTimeout());
    }

    private void reportErrorMetrics(Set<ValidationError> errors) {
        errors.forEach(error ->
                registry.counter(
                        error.getField(),
                        "type", error.getType().name(),
                        "description", error.getDescription())
                        .increment());
    }
}
