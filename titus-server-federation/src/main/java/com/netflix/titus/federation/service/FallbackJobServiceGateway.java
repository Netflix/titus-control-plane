package com.netflix.titus.federation.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;
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
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import javax.inject.Inject;

public class FallbackJobServiceGateway implements JobServiceGateway {

    private final TitusFederationConfiguration federationConfiguration;
    private final JobServiceGateway primary;
    private final JobServiceGateway secondary;

    @Inject
    public FallbackJobServiceGateway(
            TitusFederationConfiguration federationConfiguration,
            RemoteJobServiceGateway primary,
            AggregatingJobServiceGateway secondary) {

        this.federationConfiguration = federationConfiguration;
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        Observable<String> primaryObservable = primary.createJob(jobDescriptor, callMetadata);
        Observable<String> secondaryObservable = secondary.createJob(jobDescriptor, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.updateJobCapacity(jobCapacityUpdate, callMetadata);
        Completable secondaryObservable = secondary.updateJobCapacity(jobCapacityUpdate, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.updateJobCapacityWithOptionalAttributes(
                jobCapacityUpdateWithOptionalAttributes,
                callMetadata);
        Completable secondaryObservable = secondary.updateJobCapacityWithOptionalAttributes(
                jobCapacityUpdateWithOptionalAttributes,
                callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.updateJobProcesses(jobProcessesUpdate, callMetadata);
        Completable secondaryObservable = secondary.updateJobProcesses(jobProcessesUpdate, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.updateJobStatus(statusUpdate, callMetadata);
        Completable secondaryObservable = secondary.updateJobStatus(statusUpdate, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        Mono<Void> primaryMono = primary.updateJobDisruptionBudget(request, callMetadata);
        Mono<Void> secondaryMono = secondary.updateJobDisruptionBudget(request, callMetadata);
        return getFallbackMono(primaryMono, secondaryMono);
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        Mono<Void> primaryMono = primary.updateJobAttributes(request, callMetadata);
        Mono<Void> secondaryMono = secondary.updateJobAttributes(request, callMetadata);
        return getFallbackMono(primaryMono, secondaryMono);
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        Mono<Void> primaryMono = primary.deleteJobAttributes(request, callMetadata);
        Mono<Void> secondaryMono = secondary.deleteJobAttributes(request, callMetadata);
        return getFallbackMono(primaryMono, secondaryMono);
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        Observable<Job> primaryObservable = primary.findJob(jobId, callMetadata);
        Observable<Job> secondaryObservable = secondary.findJob(jobId, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        Observable<JobQueryResult> primaryObservable = primary.findJobs(jobQuery, callMetadata);
        Observable<JobQueryResult> secondaryObservable = secondary.findJobs(jobQuery, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        Observable<JobChangeNotification> primaryObservable = primary.observeJob(jobId, callMetadata);
        Observable<JobChangeNotification> secondaryObservable = secondary.observeJob(jobId, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        Observable<JobChangeNotification> primaryObservable = primary.observeJobs(query, callMetadata);
        Observable<JobChangeNotification> secondaryObservable = secondary.observeJobs(query, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.killJob(jobId, callMetadata);
        Completable secondaryObservable = secondary.killJob(jobId, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        Observable<Task> primaryObservable = primary.findTask(taskId, callMetadata);
        Observable<Task> secondaryObservable = secondary.findTask(taskId, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        Observable<TaskQueryResult> primaryObservable = primary.findTasks(taskQuery, callMetadata);
        Observable<TaskQueryResult> secondaryObservable = secondary.findTasks(taskQuery, callMetadata);
        return getFallbackObservable(primaryObservable, secondaryObservable);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.killTask(taskKillRequest, callMetadata);
        Completable secondaryObservable = secondary.killTask(taskKillRequest, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate request, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.updateTaskAttributes(request, callMetadata);
        Completable secondaryObservable = secondary.updateTaskAttributes(request, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest request, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.deleteTaskAttributes(request, callMetadata);
        Completable secondaryObservable = secondary.deleteTaskAttributes(request, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        Completable primaryCompletable = primary.moveTask(taskMoveRequest, callMetadata);
        Completable secondaryObservable = secondary.moveTask(taskMoveRequest, callMetadata);
        return getFallbackCompletable(primaryCompletable, secondaryObservable);
    }

    private <T> Observable<T> getFallbackObservable(Observable<T> primary, Observable<T> secondary) {
        if (federationConfiguration.isRemoteFederationEnabled()) {
            return primary.onErrorResumeNext(secondary);
        } else {
            return secondary;
        }
    }

    private Completable getFallbackCompletable(Completable primary, Completable secondary) {
        return getFallbackObservable(primary.toObservable(), secondary.toObservable()).toCompletable();
    }

    private <T> Mono<T> getFallbackMono(Mono<T> primary, Mono<T> secondary) {
        if (federationConfiguration.isRemoteFederationEnabled()) {
            return primary.onErrorResume(t -> secondary);
        } else {
            return secondary;
        }
    }
}
