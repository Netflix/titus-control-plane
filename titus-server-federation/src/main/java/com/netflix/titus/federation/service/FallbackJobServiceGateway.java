package com.netflix.titus.federation.service;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;

public class FallbackJobServiceGateway implements JobServiceGateway {
    public static final String METRIC_ROOT = "titus.fallbackJobServiceGateway.";
    private static final String METRIC_FALLBACK_ROOT= METRIC_ROOT + "fallbackCount.";

    private final TitusFederationConfiguration federationConfiguration;
    private final JobServiceGateway primary;
    private final JobServiceGateway secondary;
    private final TitusRuntime titusRuntime;
    private final List<Status.Code> fallbackCodes = Arrays.asList(Status.Code.UNIMPLEMENTED);

    @Inject
    public FallbackJobServiceGateway(
            TitusRuntime titusRuntime,
            TitusFederationConfiguration federationConfiguration,
            RemoteJobServiceGateway primary,
            AggregatingJobServiceGateway secondary) {

        this.titusRuntime = titusRuntime;
        this.federationConfiguration = federationConfiguration;
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor, CallMetadata callMetadata) {
        final String methodName = "createJob";
        Observable<String> primaryObservable = primary.createJob(jobDescriptor, callMetadata);
        Observable<String> secondaryObservable = secondary.createJob(jobDescriptor, callMetadata);
        return getFallbackObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate, CallMetadata callMetadata) {
        final String methodName = "updateJobCapacity";
        Completable primaryCompletable = primary.updateJobCapacity(jobCapacityUpdate, callMetadata);
        Completable secondaryCompletable = secondary.updateJobCapacity(jobCapacityUpdate, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
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
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate, CallMetadata callMetadata) {
        final String methodName = "updateJobProcesses";
        Completable primaryCompletable = primary.updateJobProcesses(jobProcessesUpdate, callMetadata);
        Completable secondaryCompletable = secondary.updateJobProcesses(jobProcessesUpdate, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate, CallMetadata callMetadata) {
        final String methodName = "updateJobStatus";
        Completable primaryCompletable = primary.updateJobStatus(statusUpdate, callMetadata);
        Completable secondaryCompletable = secondary.updateJobStatus(statusUpdate, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(JobDisruptionBudgetUpdate request, CallMetadata callMetadata) {
        final String methodName = "updateJobDisruptionBudget";
        Mono<Void> primaryMono = primary.updateJobDisruptionBudget(request, callMetadata);
        Mono<Void> secondaryMono = secondary.updateJobDisruptionBudget(request, callMetadata);
        return getFallbackMono(methodName, primaryMono, secondaryMono);
    }

    @Override
    public Mono<Void> updateJobAttributes(JobAttributesUpdate request, CallMetadata callMetadata) {
        final String methodName = "updateJobAttributes";
        Mono<Void> primaryMono = primary.updateJobAttributes(request, callMetadata);
        Mono<Void> secondaryMono = secondary.updateJobAttributes(request, callMetadata);
        return getFallbackMono(methodName, primaryMono, secondaryMono);
    }

    @Override
    public Mono<Void> deleteJobAttributes(JobAttributesDeleteRequest request, CallMetadata callMetadata) {
        final String methodName = "deleteJobAttributes";
        Mono<Void> primaryMono = primary.deleteJobAttributes(request, callMetadata);
        Mono<Void> secondaryMono = secondary.deleteJobAttributes(request, callMetadata);
        return getFallbackMono(methodName, primaryMono, secondaryMono);
    }

    @Override
    public Observable<Job> findJob(String jobId, CallMetadata callMetadata) {
        final String methodName = "findJob";
        Observable<Job> primaryObservable = primary.findJob(jobId, callMetadata);
        Observable<Job> secondaryObservable = secondary.findJob(jobId, callMetadata);
        return getFallbackObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery, CallMetadata callMetadata) {
        final String methodName = "findJobs";
        Observable<JobQueryResult> primaryObservable = primary.findJobs(jobQuery, callMetadata);
        Observable<JobQueryResult> secondaryObservable = secondary.findJobs(jobQuery, callMetadata);
        return getFallbackObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId, CallMetadata callMetadata) {
        // TODO: Direct stream APIs to the remote (primary) JobServiceGateway.
        // A failure in a stream which is in progress on the primary falling back to the secondary
        // is not the right behavior.
        return secondary.observeJob(jobId, callMetadata);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs(ObserveJobsQuery query, CallMetadata callMetadata) {
        // TODO: Direct stream APIs to the remote (primary) JobServiceGateway.
        // A failure in a stream which is in progress on the primary falling back to the secondary
        // is not the right behavior.
        return secondary.observeJobs(query, callMetadata);
    }

    @Override
    public Completable killJob(String jobId, CallMetadata callMetadata) {
        final String methodName = "killJob";
        Completable primaryCompletable = primary.killJob(jobId, callMetadata);
        Completable secondaryCompletable = secondary.killJob(jobId, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Observable<Task> findTask(String taskId, CallMetadata callMetadata) {
        final String methodName = "findTask";
        Observable<Task> primaryObservable = primary.findTask(taskId, callMetadata);
        Observable<Task> secondaryObservable = secondary.findTask(taskId, callMetadata);
        return getFallbackObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery, CallMetadata callMetadata) {
        final String methodName = "findTasks";
        Observable<TaskQueryResult> primaryObservable = primary.findTasks(taskQuery, callMetadata);
        Observable<TaskQueryResult> secondaryObservable = secondary.findTasks(taskQuery, callMetadata);
        return getFallbackObservable(methodName, primaryObservable, secondaryObservable);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest, CallMetadata callMetadata) {
        final String methodName = "killTask";
        Completable primaryCompletable = primary.killTask(taskKillRequest, callMetadata);
        Completable secondaryCompletable = secondary.killTask(taskKillRequest, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable updateTaskAttributes(TaskAttributesUpdate request, CallMetadata callMetadata) {
        final String methodName = "updateTaskAttributes";
        Completable primaryCompletable = primary.updateTaskAttributes(request, callMetadata);
        Completable secondaryCompletable = secondary.updateTaskAttributes(request, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable deleteTaskAttributes(TaskAttributesDeleteRequest request, CallMetadata callMetadata) {
        final String methodName = "deleteTaskAttributes";
        Completable primaryCompletable = primary.deleteTaskAttributes(request, callMetadata);
        Completable secondaryCompletable = secondary.deleteTaskAttributes(request, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    @Override
    public Completable moveTask(TaskMoveRequest taskMoveRequest, CallMetadata callMetadata) {
        final String methodName = "moveTask";
        Completable primaryCompletable = primary.moveTask(taskMoveRequest, callMetadata);
        Completable secondaryCompletable = secondary.moveTask(taskMoveRequest, callMetadata);
        return getFallbackCompletable(methodName, primaryCompletable, secondaryCompletable);
    }

    private <T> Observable<T> getFallbackObservable(String methodName, Observable<T> primary, Observable<T> secondary) {
        if (federationConfiguration.isRemoteFederationEnabled()) {
            return primary.onErrorResumeNext( (t) -> {
                incrementFallbackCounter(methodName, t);
                if (shouldFallback(t)) {
                    return secondary;
                } else {
                    return Observable.error(t);
                }
            });
        } else {
            return secondary;
        }
    }

    private <T> Mono<T> getFallbackMono(String methodName, Mono<T> primary, Mono<T> secondary) {
        if (federationConfiguration.isRemoteFederationEnabled()) {
            return primary.onErrorResume(t -> {
                incrementFallbackCounter(methodName, t);
                if (shouldFallback(t)) {
                    return secondary;
                } else {
                    return Mono.error(t);
                }
            });
        } else {
            return secondary;
        }
    }

    private Completable getFallbackCompletable(String methodName, Completable primary, Completable secondary) {
        return getFallbackObservable(methodName, primary.toObservable(), secondary.toObservable()).toCompletable();
    }

    private boolean shouldFallback(Throwable t) {
        return t instanceof StatusRuntimeException && fallbackCodes.contains(Status.fromThrowable(t).getCode());
    }

    private void incrementFallbackCounter(String methodName, Throwable t) {
        Id metricId = this.getMetricId(METRIC_FALLBACK_ROOT + methodName, t);
        this.titusRuntime.getRegistry().counter(metricId).increment();
    }

    private Id getMetricId(String metricName, Throwable t) {
        Status.Code code = Status.fromThrowable(t).getCode();
        String reason = code == Status.Code.UNKNOWN ? t.getClass().getSimpleName() : code.toString();

        return this.titusRuntime.getRegistry().createId(
                metricName,
                "reason", reason);
    }
}
