/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallMetadataConstants;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.KeepAliveRequest;
import com.netflix.titus.grpc.protogen.KeepAliveResponse;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.ObserveJobsWithKeepAliveRequest;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3JobQueryCriteriaEvaluator;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.master.jobmanager.endpoint.v3.grpc.ObserveJobsContext.SNAPSHOT_END_MARKER;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toJobQueryCriteria;

class ObserveJobsSubscription {

    private static final Logger logger = LoggerFactory.getLogger(ObserveJobsSubscription.class);

    private final ObserveJobsContext context;
    private final DefaultJobManagementServiceGrpcMetrics metrics;
    private final TitusRuntime titusRuntime;

    // GRPC channel
    private final BlockingQueue<ObserveJobsWithKeepAliveRequest> grpcClientEvents = new LinkedBlockingDeque<>();
    private volatile StreamObserver<JobChangeNotification> grpcResponseObserver;
    private volatile boolean grpcStreamInitiated;
    private volatile boolean grpcSnapshotMarkerSent;
    private volatile boolean grpcStreamCancelled;

    // Job service
    private final BlockingQueue<JobChangeNotification> jobServiceEvents = new LinkedBlockingDeque<>();
    private volatile Throwable jobServiceError;
    private volatile boolean jobServiceCompleted;
    @VisibleForTesting
    volatile Subscription jobServiceSubscription;

    private final AtomicLong wip = new AtomicLong();

    ObserveJobsSubscription(ObserveJobsContext context) {
        this.context = context;
        this.metrics = context.getMetrics();
        this.titusRuntime = context.getTitusRuntime();
    }

    void observeJobs(ObserveJobsQuery query, StreamObserver<JobChangeNotification> responseObserver) {
        grpcClientEvents.add(ObserveJobsWithKeepAliveRequest.newBuilder()
                .setQuery(query)
                .build()
        );
        connect(responseObserver);
        drain();
    }

    StreamObserver<ObserveJobsWithKeepAliveRequest> observeJobsWithKeepAlive(StreamObserver<JobChangeNotification> responseObserver) {
        connect(responseObserver);
        return new StreamObserver<ObserveJobsWithKeepAliveRequest>() {
            @Override
            public void onNext(ObserveJobsWithKeepAliveRequest request) {
                grpcClientEvents.add(request);
                drain();
            }

            @Override
            public void onError(Throwable error) {
                grpcStreamCancelled = true;
                drain();
            }

            @Override
            public void onCompleted() {
                // It is ok that the GRPC input stream is closed. We will continue sending events to the client.
            }
        };
    }

    private void connect(StreamObserver<JobChangeNotification> responseObserver) {
        this.grpcResponseObserver = responseObserver;
        ServerCallStreamObserver<JobChangeNotification> serverObserver = (ServerCallStreamObserver<JobChangeNotification>) responseObserver;
        serverObserver.setOnCancelHandler(() -> {
            grpcStreamCancelled = true;
            drain();
        });
    }

    private void drain() {
        try {
            drainInternal();
        } catch (Throwable error) {
            logger.error("Unexpected error in the job event stream", error);
            grpcStreamCancelled = true;
            checkTerminated(true, true);
        }
    }

    /**
     * Based on: https://akarnokd.blogspot.com/2015/05/operator-concurrency-primitives_9.html
     */
    private void drainInternal() {
        if (wip.getAndIncrement() == 0) {
            do {
                if (checkTerminated(jobServiceCompleted, jobServiceEvents.isEmpty())) {
                    return;
                }

                wip.lazySet(1);

                if (grpcStreamInitiated || tryInitialize()) {
                    while (true) {
                        boolean completed = jobServiceCompleted;
                        JobChangeNotification jobServiceEvent = jobServiceEvents.poll();
                        if (checkTerminated(completed, jobServiceEvent == null)) {
                            return;
                        } else if (jobServiceEvent != null) {
                            if (jobServiceEvent.getNotificationCase() == JobChangeNotification.NotificationCase.SNAPSHOTEND) {
                                this.grpcSnapshotMarkerSent = true;
                            }
                        } else if (grpcSnapshotMarkerSent) {
                            // No more job service events to send. We can drain the GRPC input stream to process
                            // keep alive requests.
                            KeepAliveRequest keepAliveRequest = getLastKeepAliveEvent();
                            if (keepAliveRequest == null) {
                                break;
                            }
                            jobServiceEvent = toGrpcKeepAliveResponse(keepAliveRequest);
                        }

                        grpcResponseObserver.onNext(jobServiceEvent);
                    }
                }
            } while (wip.decrementAndGet() != 0);
        }
    }

    private boolean tryInitialize() {
        ObserveJobsQuery query = getLastObserveJobsQueryEvent();
        if (query == null) {
            return false;
        }

        Stopwatch start = Stopwatch.createStarted();

        String trxId = UUID.randomUUID().toString();
        CallMetadata callMetadata = context.getCallMetadataResolver().resolve().orElse(CallMetadataConstants.UNDEFINED_CALL_METADATA);
        metrics.observeJobsStarted(trxId, callMetadata);

        JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria = toJobQueryCriteria(query);
        V3JobQueryCriteriaEvaluator jobsPredicate = new V3JobQueryCriteriaEvaluator(criteria, titusRuntime);
        V3TaskQueryCriteriaEvaluator tasksPredicate = new V3TaskQueryCriteriaEvaluator(criteria, titusRuntime);

        Observable<JobChangeNotification> eventStream = context.getJobOperations().observeJobs(jobsPredicate, tasksPredicate)
                // avoid clogging the computation scheduler
                .observeOn(context.getObserveJobsScheduler())
                .subscribeOn(context.getObserveJobsScheduler(), false)
                .map(event -> GrpcJobManagementModelConverters.toGrpcJobChangeNotification(event, context.getGrpcObjectsCache(), titusRuntime.getClock().wallTime()))
                .compose(ObservableExt.head(() -> {
                    List<JobChangeNotification> snapshot = createJobsSnapshot(jobsPredicate, tasksPredicate);
                    snapshot.add(SNAPSHOT_END_MARKER);
                    return snapshot;
                }))
                .doOnError(e -> logger.error("Unexpected error in jobs event stream", e));

        AtomicBoolean closingProcessed = new AtomicBoolean();
        this.jobServiceSubscription = eventStream
                .doOnUnsubscribe(() -> {
                    if (!closingProcessed.getAndSet(true)) {
                        metrics.observeJobsUnsubscribed(trxId, start.elapsed(TimeUnit.MILLISECONDS));
                    }
                })
                .subscribe(
                        event -> {
                            metrics.observeJobsEventEmitted(trxId);
                            jobServiceEvents.add(event);
                            drain();
                        },
                        e -> {
                            if (!closingProcessed.getAndSet(true)) {
                                metrics.observeJobsError(trxId, start.elapsed(TimeUnit.MILLISECONDS), e);
                            }
                            jobServiceCompleted = true;
                            jobServiceError = new StatusRuntimeException(Status.INTERNAL
                                    .withDescription("All jobs monitoring stream terminated with an error")
                                    .withCause(e));
                            drain();
                        },
                        () -> {
                            if (!closingProcessed.getAndSet(true)) {
                                metrics.observeJobsCompleted(trxId, start.elapsed(TimeUnit.MILLISECONDS));
                            }
                            jobServiceCompleted = true;
                            drain();
                        }
                );
        this.grpcStreamInitiated = true;
        return true;
    }

    private ObserveJobsQuery getLastObserveJobsQueryEvent() {
        ObserveJobsQuery jobsQuery = null;
        ObserveJobsWithKeepAliveRequest event;
        while ((event = grpcClientEvents.poll()) != null) {
            if (event.getKindCase() == ObserveJobsWithKeepAliveRequest.KindCase.QUERY) {
                jobsQuery = event.getQuery();
            }
        }
        return jobsQuery;
    }

    private KeepAliveRequest getLastKeepAliveEvent() {
        KeepAliveRequest keepAliveRequest = null;
        ObserveJobsWithKeepAliveRequest event;
        while ((event = grpcClientEvents.poll()) != null) {
            if (event.getKindCase() == ObserveJobsWithKeepAliveRequest.KindCase.KEEPALIVEREQUEST) {
                keepAliveRequest = event.getKeepAliveRequest();
            }
        }
        return keepAliveRequest;
    }

    private boolean checkTerminated(boolean isDone, boolean isEmpty) {
        if (grpcStreamCancelled) {
            ObservableExt.safeUnsubscribe(jobServiceSubscription);
            return true;
        }
        if (isDone) {
            Throwable e = jobServiceError;
            if (e != null) {
                jobServiceEvents.clear();
                ExceptionExt.silent(() -> grpcResponseObserver.onError(e));
                return true;
            } else if (isEmpty) {
                ExceptionExt.silent(() -> grpcResponseObserver.onCompleted());
                return true;
            }
        }
        return false;
    }

    private List<JobChangeNotification> createJobsSnapshot(
            Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
            Predicate<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, com.netflix.titus.api.jobmanager.model.job.Task>> tasksPredicate) {
        long now = titusRuntime.getClock().wallTime();
        List<JobChangeNotification> snapshot = new ArrayList<>();

        // Generics casting issue
        List allJobsAndTasksRaw = context.getJobOperations().getJobsAndTasks();
        List<Pair<com.netflix.titus.api.jobmanager.model.job.Job<?>, List<com.netflix.titus.api.jobmanager.model.job.Task>>> allJobsAndTasks = allJobsAndTasksRaw;
        allJobsAndTasks.forEach(pair -> {
            com.netflix.titus.api.jobmanager.model.job.Job<?> job = pair.getLeft();
            List<com.netflix.titus.api.jobmanager.model.job.Task> tasks = pair.getRight();
            if (jobsPredicate.test(pair)) {
                snapshot.add(context.toJobChangeNotification(job, now));
            }
            tasks.forEach(task -> {
                if (tasksPredicate.test(Pair.of(job, task))) {
                    snapshot.add(context.toJobChangeNotification(task, now));
                }
            });
        });

        return snapshot;
    }

    private JobChangeNotification toGrpcKeepAliveResponse(KeepAliveRequest keepAliveRequest) {
        return JobChangeNotification.newBuilder()
                .setKeepAliveResponse(KeepAliveResponse.newBuilder()
                        .setRequest(keepAliveRequest)
                        .setTimestamp(titusRuntime.getClock().wallTime())
                        .build()
                )
                .build();
    }
}
