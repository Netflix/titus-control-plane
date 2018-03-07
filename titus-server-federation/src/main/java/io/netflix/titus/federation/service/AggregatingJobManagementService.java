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
import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.grpc.SessionContext;
import io.netflix.titus.common.util.concurrency.CallbackCountDownLatch;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.getObserveJobsMethod;
import static io.netflix.titus.common.grpc.GrpcUtil.callStreaming;

@Singleton
public class AggregatingJobManagementService implements JobManagementService {
    private final CellConnector connector;
    private final SessionContext sessionContext;

    public AggregatingJobManagementService(CellConnector connector, SessionContext sessionContext) {
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
        return Observable.create(emitter -> {
            Map<Cell, JobManagementServiceStub> clients = CellConnectorUtil.stubs(connector, JobManagementServiceGrpc::newStub);
            final CountDownLatch markersEmitted = new CallbackCountDownLatch(clients.size(),
                    () -> emitter.onNext(buildJobSnapshotEndMarker())
            );

            ClientCall[] clientCalls = clients.entrySet().stream().map(entry -> {
                JobManagementServiceStub client = entry.getValue();
                StreamObserver<JobChangeNotification> streamObserver = new FilterOutFirstMarker(emitter, markersEmitted);
                return callStreaming(sessionContext, client, getObserveJobsMethod(), Empty.getDefaultInstance(), streamObserver);
            }).toArray(ClientCall[]::new);

            GrpcUtil.attachCancellingCallback(emitter, clientCalls);
        }, Emitter.BackpressureMode.NONE);
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

    private static StatusException notImplemented(String operation) {
        return Status.UNIMPLEMENTED.withDescription(operation + " is not implemented").asException();
    }

    private static JobChangeNotification buildJobSnapshotEndMarker() {
        final JobChangeNotification.SnapshotEnd marker = JobChangeNotification.SnapshotEnd.newBuilder().build();
        return JobChangeNotification.newBuilder().setSnapshotEnd(marker).build();
    }
}
