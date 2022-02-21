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

public class SwitchingJobServiceGateway implements JobServiceGateway {

    private final TitusFederationConfiguration federationConfiguration;
    private final JobServiceGateway primary;
    private final JobServiceGateway secondary;

    @Inject
    public SwitchingJobServiceGateway(
            TitusFederationConfiguration federationConfiguration,
            RemoteJobServiceGateway primary,
            AggregatingJobServiceGateway secondary) {

        this.federationConfiguration = federationConfiguration;
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        final String methodName = "createJob";
        Observable<String> primaryObservable = primary.createJob(jobDescriptor, callMetadata);
        Observable<String> secondaryObservable = secondary.createJob(jobDescriptor, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        final String methodName = "updateJobCapacity";
        Completable primaryCompletable = primary.updateJobCapacity(jobCapacityUpdate, callMetadata);
        Completable secondaryCompletable = secondary.updateJobCapacity(jobCapacityUpdate, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateJobCapacityWithOptionalAttributes(JobCapacityUpdateWithOptionalAttributes jobCapacityUpdateWithOptionalAttributes, CallMetadata callMetadata) {
        final String methodName = "updateJobCapacityWithOptionalAttributes";
        Completable primaryCompletable = primary.updateJobCapacityWithOptionalAttributes(
                jobCapacityUpdateWithOptionalAttributes,
                callMetadata);
        Completable secondaryCompletable = secondary.updateJobCapacityWithOptionalAttributes(
                jobCapacityUpdateWithOptionalAttributes,
                callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        final String methodName = "updateJobProcesses";
        Completable primaryCompletable = primary.updateJobProcesses(jobProcessesUpdate, callMetadata);
        Completable secondaryCompletable = secondary.updateJobProcesses(jobProcessesUpdate, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        final String methodName = "updateJobStatus";
        Completable primaryCompletable = primary.updateJobStatus(statusUpdate, callMetadata);
        Completable secondaryCompletable = secondary.updateJobStatus(statusUpdate, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        final String methodName = "updateJobDisruptionBudget";
        Mono<Void> primaryMono = primary.updateJobDisruptionBudget(request, callMetadata);
        Mono<Void> secondaryMono = secondary.updateJobDisruptionBudget(request, callMetadata);
        return getSwitchingMono(methodName, primaryMono, secondaryMono);
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        final String methodName = "updateJobAttributes";
        Mono<Void> primaryMono = primary.updateJobAttributes(request, callMetadata);
        Mono<Void> secondaryMono = secondary.updateJobAttributes(request, callMetadata);
        return getSwitchingMono(methodName, primaryMono, secondaryMono);
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        final String methodName = "deleteJobAttributes";
        Mono<Void> primaryMono = primary.deleteJobAttributes(request, callMetadata);
        Mono<Void> secondaryMono = secondary.deleteJobAttributes(request, callMetadata);
        return getSwitchingMono(methodName, primaryMono, secondaryMono);
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        final String methodName = "findJob";
        Observable<Job> primaryObservable = primary.findJob(jobId, callMetadata);
        Observable<Job> secondaryObservable = secondary.findJob(jobId, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        final String methodName = "findJobs";
        Observable<JobQueryResult> primaryObservable = primary.findJobs(jobQuery, callMetadata);
        Observable<JobQueryResult> secondaryObservable = secondary.findJobs(jobQuery, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        final String methodName = "observeJob";
        Observable<JobChangeNotification> primaryObservable = primary.observeJob(jobId, callMetadata);
        Observable<JobChangeNotification> secondaryObservable = secondary.observeJob(jobId, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        final String methodName = "observeJobs";
        Observable<JobChangeNotification> primaryObservable = primary.observeJobs(query, callMetadata);
        Observable<JobChangeNotification> secondaryObservable = secondary.observeJobs(query, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        final String methodName = "killJob";
        Completable primaryCompletable = primary.killJob(jobId, callMetadata);
        Completable secondaryCompletable = secondary.killJob(jobId, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        final String methodName = "findTask";
        Observable<Task> primaryObservable = primary.findTask(taskId, callMetadata);
        Observable<Task> secondaryObservable = secondary.findTask(taskId, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        final String methodName = "findTasks";
        Observable<TaskQueryResult> primaryObservable = primary.findTasks(taskQuery, callMetadata);
        Observable<TaskQueryResult> secondaryObservable = secondary.findTasks(taskQuery, callMetadata);
        return getSwitchingObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        final String methodName = "killTask";
        Completable primaryCompletable = primary.killTask(taskKillRequest, callMetadata);
        Completable secondaryCompletable = secondary.killTask(taskKillRequest, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate request, CallMetadata callMetadata) {
        final String methodName = "updateTaskAttributes";
        Completable primaryCompletable = primary.updateTaskAttributes(request, callMetadata);
        Completable secondaryCompletable = secondary.updateTaskAttributes(request, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest request, CallMetadata callMetadata) {
        final String methodName = "deleteTaskAttributes";
        Completable primaryCompletable = primary.deleteTaskAttributes(request, callMetadata);
        Completable secondaryCompletable = secondary.deleteTaskAttributes(request, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        final String methodName = "moveTask";
        Completable primaryCompletable = primary.moveTask(taskMoveRequest, callMetadata);
        Completable secondaryCompletable = secondary.moveTask(taskMoveRequest, callMetadata);
        return getSwitchingCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    private <T> Observable<T> getSwitchingObservable(String methodName, Observable<T> primary, Observable<T> secondary) {
        if (federationConfiguration.isRemoteFederationEnabled()) {
            return primary;
        } else {
            return secondary;
        }
    }

    private <T> Mono<T> getSwitchingMono(String methodName, Mono<T> primary, Mono<T> secondary) {
        if (federationConfiguration.isRemoteFederationEnabled()) {
            return primary;
        } else {
            return secondary;
        }
    }

    private Completable getSwitchingCompletable(String methodName, Completable primary, Completable secondary) {
        return getSwitchingObservable(methodName, primary.toObservable(), secondary.toObservable()).toCompletable();
    }
}
