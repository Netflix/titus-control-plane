package com.netflix.titus.runtime.connector.jobmanager;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import rx.Completable;
import rx.Observable;

public class JobManagementClientDelegate implements JobManagementClient {

    private final JobManagementClient delegate;

    public JobManagementClientDelegate(JobManagementClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        return delegate.createJob(jobDescriptor);
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        return delegate.updateJobCapacity(jobCapacityUpdate);
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate) {
        return delegate.updateJobProcesses(jobProcessesUpdate);
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate) {
        return delegate.updateJobStatus(statusUpdate);
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        return delegate.findJob(jobId);
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery) {
        return delegate.findJobs(jobQuery);
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return delegate.observeJob(jobId);
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        return delegate.observeJobs();
    }

    @Override
    public Completable killJob(String jobId) {
        return delegate.killJob(jobId);
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        return delegate.findTask(taskId);
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery) {
        return delegate.findTasks(taskQuery);
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest) {
        return delegate.killTask(taskKillRequest);
    }
}
