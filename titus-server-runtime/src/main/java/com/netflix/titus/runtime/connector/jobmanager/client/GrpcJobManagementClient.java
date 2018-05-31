package com.netflix.titus.runtime.connector.jobmanager.client;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.validation.ConstraintViolation;

import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
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
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestCompletable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createSimpleClientResponseObserver;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

/**
 * {@link JobManagementClient} implementation that connects to TitusMaster over the GRPC channel.
 */
public class GrpcJobManagementClient implements JobManagementClient {

    private final JobManagementServiceGrpc.JobManagementServiceStub client;
    private final CallMetadataResolver callMetadataResolver;
    private final EntitySanitizer entitySanitizer;
    private final GrpcClientConfiguration configuration;

    public GrpcJobManagementClient(JobManagementServiceGrpc.JobManagementServiceStub client,
                                   CallMetadataResolver callMetadataResolver,
                                   EntitySanitizer entitySanitizer,
                                   GrpcClientConfiguration configuration) {
        this.client = client;
        this.callMetadataResolver = callMetadataResolver;
        this.entitySanitizer = entitySanitizer;
        this.configuration = configuration;
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

        Set<ConstraintViolation<com.netflix.titus.api.jobmanager.model.job.JobDescriptor>> violations = entitySanitizer.validate(sanitizedCoreJobDescriptor);
        if (!violations.isEmpty()) {
            return Observable.error(TitusServiceException.invalidArgument(violations));
        }

        JobDescriptor effectiveJobDescriptor = V3GrpcModelConverters.toGrpcJobDescriptor(sanitizedCoreJobDescriptor);

        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            createWrappedStub(client, callMetadataResolver, configuration.getRequestTimeout()).createJob(effectiveJobDescriptor, streamObserver);
        }, configuration.getRequestTimeout());
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        Capacity newCapacity = V3GrpcModelConverters.toCoreCapacity(jobCapacityUpdate.getCapacity());
        Set<ConstraintViolation<Capacity>> violations = entitySanitizer.validate(newCapacity);
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
}
