package com.netflix.titus.federation.service;

import com.google.protobuf.Empty;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.federation.service.router.CellRouter;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
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
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
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
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.UUID;
import java.util.function.BiConsumer;

import static com.netflix.titus.api.jobmanager.JobAttributes.*;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createRequestObservable;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class RemoteJobServiceGateway implements JobServiceGateway {

    private final JobManagementServiceStub remoteClient;
    private final GrpcConfiguration grpcConfiguration;
    private final TitusFederationConfiguration federationConfiguration;
    private final CellRouter cellRouter;
    private static final Cell unknownCell = new Cell("UNKNOWN", "UNKNOWN");

    @Inject
    public RemoteJobServiceGateway(
            TitusFederationConfiguration federationConfiguration,
            RemoteFederationConnector fedConnector,
            CellRouter cellRouter,
            GrpcConfiguration grpcConfiguration) {
        this.federationConfiguration = federationConfiguration;
        this.remoteClient = JobManagementServiceGrpc.newStub(fedConnector.getChannel());
        this.cellRouter = cellRouter;
        this.grpcConfiguration = grpcConfiguration;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        // We want to know where the job would have been routed so remote federation can compare it to its own routing
        // decision.
        Cell cell = cellRouter.routeKey(jobDescriptor).orElse(unknownCell);
        String federatedJobId = UUID.randomUUID().toString();

        JobDescriptor jd = addAttributes(
                jobDescriptor.toBuilder(),
                federatedJobId,
                cell.getName()).build();

        return createRequestObservable(emitter -> {
            StreamObserver<JobId> streamObserver = GrpcUtil.createClientResponseObserver(
                    emitter,
                    jobId -> emitter.onNext(jobId.getId()),
                    emitter::onError,
                    emitter::onCompleted
            );
            wrap(remoteClient, callMetadata, grpcConfiguration.getRequestTimeoutMs()).createJob(jd, streamObserver);
        }, grpcConfiguration.getRequestTimeoutMs());
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateJobCapacity(jobCapacityUpdate, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateJobCapacityWithOptionalAttributes(jobCapacityUpdateWithOptionalAttributes, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateJobProcesses(jobProcessesUpdate, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateJobStatus(statusUpdate, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        return remoteCallReact(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateJobDisruptionBudget(request, streamObserver), callMetadata)
                .ignoreElement().cast(Void.class);
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        return remoteCallReact(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateJobAttributes(request, streamObserver), callMetadata)
                .ignoreElement().cast(Void.class);
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        return remoteCallReact(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.deleteJobAttributes(request, streamObserver), callMetadata)
                .ignoreElement().cast(Void.class);
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Job> streamObserver) ->
                        client.findJob(JobId.newBuilder().setId(jobId).build(), streamObserver), callMetadata);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<JobQueryResult> streamObserver) ->
                        client.findJobs(jobQuery, streamObserver), callMetadata);
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        return remoteCallWithNoDeadline(
                (JobManagementServiceStub client, StreamObserver<JobChangeNotification> streamObserver) ->
                        client.observeJob(JobId.newBuilder().setId(jobId).build(), streamObserver), callMetadata);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        return remoteCallWithNoDeadline(
                (JobManagementServiceStub client, StreamObserver<JobChangeNotification> streamObserver) ->
                        client.observeJobs(query, streamObserver), callMetadata);
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.killJob(JobId.newBuilder().setId(jobId).build(), streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Task> streamObserver) ->
                        client.findTask(TaskId.newBuilder().setId(taskId).build(), streamObserver), callMetadata);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<TaskQueryResult> streamObserver) ->
                        client.findTasks(taskQuery, streamObserver), callMetadata);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.killTask(taskKillRequest, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate request, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.updateTaskAttributes(request, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest request, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.deleteTaskAttributes(request, streamObserver), callMetadata)
                .toCompletable();
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        return remoteCall(
                (JobManagementServiceStub client, StreamObserver<Empty> streamObserver) ->
                        client.moveTask(taskMoveRequest, streamObserver), callMetadata)
                .toCompletable();
    }

    private JobManagementServiceStub wrap(JobManagementServiceStub client, CallMetadata callMetadata, long timeoutMs) {
        return createWrappedStub(client, callMetadata, timeoutMs);
    }

    private JobManagementServiceStub wrapWithNoDeadline(JobManagementServiceStub client, CallMetadata callMetadata) {
        return createWrappedStub(client, callMetadata);
    }

    private interface ClientCall<T> extends BiConsumer<JobManagementServiceStub, StreamObserver<T>> {
        // generics sanity
    }

    private static <STUB extends AbstractStub<STUB>, RespT> Observable<RespT> callToRemote(
            STUB client,
            BiConsumer<STUB, StreamObserver<RespT>> fnCall) {
        return GrpcUtil.createRequestObservable(emitter -> {
            StreamObserver<RespT> streamObserver = GrpcUtil.createSimpleClientResponseObserver(emitter);
            fnCall.accept(client, streamObserver);
        });
    }

    private <T> Observable<T> remoteCall(ClientCall<T> clientCall, CallMetadata callMetadata) {
        return callToRemote(
                remoteClient,
                (client, streamObserver) -> clientCall.accept(
                        wrap(remoteClient, callMetadata, this.grpcConfiguration.getRequestTimeoutMs()),
                        streamObserver));
    }

    private <T> Observable<T> remoteCallWithNoDeadline(ClientCall<T> clientCall, CallMetadata callMetadata) {
        return callToRemote(
                remoteClient,
                (client, streamObserver) -> clientCall.accept(
                        wrapWithNoDeadline(remoteClient, callMetadata),
                        streamObserver));
    }

    private <T> Mono<T> remoteCallReact(ClientCall<T> clientCall, CallMetadata callMetadata) {
        return ReactorExt.toMono(remoteCall(clientCall, callMetadata).toSingle());
    }

    private JobDescriptor.Builder addAttributes(JobDescriptor.Builder jobDescriptorBuilder, String federatedJobId, String routingCell) {
        return jobDescriptorBuilder.putAllAttributes(CollectionsExt.<String, String>newHashMap()
                .entry(JOB_ATTRIBUTES_STACK, federationConfiguration.getStack())
                .entry(JOB_ATTRIBUTES_FEDERATED_JOB_ID, federatedJobId)
                .entry(JOB_ATTRIBUTE_ROUTING_CELL, routingCell)
                .toMap());
    }
}
