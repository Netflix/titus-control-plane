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

package io.netflix.titus.federation.service;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdate;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.JobProcessesUpdate;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import com.netflix.titus.grpc.protogen.JobStatusUpdate;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskKillRequest;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskQueryResult;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.common.grpc.EmitterWithMultipleSubscriptions;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.util.concurrency.CallbackCountDownLatch;
import io.netflix.titus.federation.startup.TitusFederationConfiguration;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import static io.netflix.titus.api.jobmanager.model.job.JobModel.STACK_NAME_KEY;
import static io.netflix.titus.common.grpc.GrpcUtil.createRequestObservable;
import static io.netflix.titus.common.grpc.GrpcUtil.createWrappedStub;

@Singleton
public class AggregatingJobManagementService implements JobManagementService {
    private final TitusFederationConfiguration configuration;
    private final CellConnector connector;
    private final SessionContext sessionContext;

    @Inject
    public AggregatingJobManagementService(TitusFederationConfiguration configuration, CellConnector connector, SessionContext sessionContext) {
        this.configuration = configuration;
        this.connector = connector;
        this.sessionContext = sessionContext;
    }

    @Override
    public Observable<String> createJob(JobDescriptor jobDescriptor) {
        return Observable.error(notImplemented("createJob"));
    }

    @Override
    public Completable updateJobCapacity(JobCapacityUpdate jobCapacityUpdate) {
        return Completable.error(notImplemented("updateJobCapacity"));
    }

    @Override
    public Completable updateJobProcesses(JobProcessesUpdate jobProcessesUpdate) {
        return Completable.error(notImplemented("updateJobProcesses"));
    }

    @Override
    public Completable updateJobStatus(JobStatusUpdate statusUpdate) {
        return Completable.error(notImplemented("updateJobStatus"));
    }

    @Override
    public Observable<Job> findJob(String jobId) {
        return Observable.error(notImplemented("findJob"));
    }

    @Override
    public Observable<JobQueryResult> findJobs(JobQuery jobQuery) {
        return Observable.error(notImplemented("findJobs"));
    }

    @Override
    public Observable<JobChangeNotification> observeJob(String jobId) {
        return Observable.error(notImplemented("observeJob"));
    }

    @Override
    public Observable<JobChangeNotification> observeJobs() {
        final Observable<JobChangeNotification> observable = createRequestObservable(delegate -> {
            Emitter<JobChangeNotification> emitter = new EmitterWithMultipleSubscriptions<>(delegate);
            Map<Cell, JobManagementServiceStub> clients = CellConnectorUtil.stubs(connector, JobManagementServiceGrpc::newStub);
            final CountDownLatch markersEmitted = new CallbackCountDownLatch(clients.size(),
                    () -> emitter.onNext(buildJobSnapshotEndMarker())
            );
            clients.forEach((cell, client) -> {
                StreamObserver<JobChangeNotification> streamObserver = new FilterOutFirstMarker(emitter, markersEmitted);
                createWrappedStub(client, sessionContext).observeJobs(Empty.getDefaultInstance(), streamObserver);
            });
        });
        return observable.map(this::addStackName);
    }

    @Override
    public Completable killJob(String jobId) {
        return Completable.error(notImplemented("killJob"));
    }

    @Override
    public Observable<Task> findTask(String taskId) {
        return Observable.error(notImplemented("findTask"));
    }

    @Override
    public Observable<TaskQueryResult> findTasks(TaskQuery taskQuery) {
        return Observable.error(notImplemented("findTasks"));
    }

    @Override
    public Completable killTask(TaskKillRequest taskKillRequest) {
        return Completable.error(notImplemented("killTask"));
    }

    private JobChangeNotification addStackName(JobChangeNotification notification) {
        switch (notification.getNotificationCase()) {
            case JOBUPDATE:
                JobDescriptor jobDescriptor = notification.getJobUpdate().getJob().getJobDescriptor().toBuilder()
                        .putAttributes(STACK_NAME_KEY, configuration.getStack())
                        .build();
                Job job = notification.getJobUpdate().getJob().toBuilder().setJobDescriptor(jobDescriptor).build();
                JobChangeNotification.JobUpdate jobUpdate = notification.getJobUpdate().toBuilder().setJob(job).build();
                return notification.toBuilder().setJobUpdate(jobUpdate).build();
            case TASKUPDATE:
                // TODO(fabio): decorate tasks
                return notification;
            default:
                return notification;
        }
    }

    private static StatusException notImplemented(String operation) {
        return Status.UNIMPLEMENTED.withDescription(operation + " is not implemented").asException();
    }

    private static JobChangeNotification buildJobSnapshotEndMarker() {
        final JobChangeNotification.SnapshotEnd marker = JobChangeNotification.SnapshotEnd.newBuilder().build();
        return JobChangeNotification.newBuilder().setSnapshotEnd(marker).build();
    }
}

