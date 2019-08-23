/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.supplementary.taskspublisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceFutureStub;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.supplementary.taskspublisher.es.EsTaskPublisherMetrics;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor.CALLER_ID_KEY;

public class TitusClientImpl implements TitusClient {
    private static final Logger logger = LoggerFactory.getLogger(TitusClientImpl.class);
    private static final String CLIENT_ID = "tasksPublisher";
    private static final int MAX_CACHE_SIZE = 40000;
    private final JobManagementServiceStub jobManagementService;
    private final JobManagementServiceFutureStub jobManagementServiceFutureStub;
    private final Registry registry;
    private final AtomicInteger numJobUpdates = new AtomicInteger(0);
    private final AtomicInteger numTaskUpdates = new AtomicInteger(0);
    private final AtomicInteger numSnapshotUpdates = new AtomicInteger(0);
    private final AtomicInteger numMissingJobUpdate = new AtomicInteger(0);
    private final AtomicInteger apiErrors = new AtomicInteger(0);

    private final AsyncLoadingCache<String, Job> jobs;

    public TitusClientImpl(JobManagementServiceStub jobManagementService,
                           JobManagementServiceFutureStub jobManagementServiceFutureStub,
                           Registry registry) {
        this.jobManagementService = jobManagementService;
        this.jobManagementServiceFutureStub = jobManagementServiceFutureStub;
        this.registry = registry;
        jobs = buildCacheForJobs();
        configureMetrics();
    }

    @Override
    public Mono<Task> getTask(String taskId) {
        logger.debug("Getting Task information about taskId {}", taskId);
        return Mono.create(sink -> attachCallerId(jobManagementService, CLIENT_ID)
                .findTask(TaskId.newBuilder().setId(taskId).build(), new StreamObserver<Task>() {
                    @Override
                    public void onNext(Task task) {
                        sink.success(task);
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("Error fetching task information for task ID = {}", taskId);
                        apiErrors.incrementAndGet();
                        sink.error(t);
                    }

                    @Override
                    public void onCompleted() {

                    }
                }));
    }

    @Override
    public Flux<JobOrTaskUpdate> getJobAndTaskUpdates() {
        return Flux.create(sink -> attachCallerId(jobManagementService, CLIENT_ID)
                .observeJobs(ObserveJobsQuery.newBuilder().build(), new StreamObserver<JobChangeNotification>() {
                    @Override
                    public void onNext(JobChangeNotification jobChangeNotification) {
                        switch (jobChangeNotification.getNotificationCase()) {
                            case JOBUPDATE:
                                final Job job = jobChangeNotification.getJobUpdate().getJob();
                                jobs.put(job.getId(), CompletableFuture.completedFuture(job));
                                logger.debug("<{}> JobUpdate {}", Thread.currentThread().getName(), jobChangeNotification.getJobUpdate().getJob().getId());
                                sink.next(JobOrTaskUpdate.jobUpdate(job));
                                numJobUpdates.incrementAndGet();
                                break;
                            case TASKUPDATE:
                                logger.debug("<{}> TaskUpdate {}", Thread.currentThread().getName(), jobChangeNotification.getTaskUpdate().getTask().getId());
                                final Task task = jobChangeNotification.getTaskUpdate().getTask();
                                sink.next(JobOrTaskUpdate.taskUpdate(task));
                                numTaskUpdates.incrementAndGet();
                                break;
                            case SNAPSHOTEND:
                                logger.info("<{}> SnapshotEnd {}", Thread.currentThread().getName(), jobChangeNotification);
                                numSnapshotUpdates.incrementAndGet();
                                break;
                            default:
                                logger.error("<{}> Unknown Notification ? {}", Thread.currentThread().getName(), jobChangeNotification.getNotificationCase());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("Exception in ObserveJobs :: ", t);
                        apiErrors.incrementAndGet();
                        sink.error(t);
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("STREAM completed ?");
                        sink.complete();
                    }
                }));
    }

    @Override
    public Mono<Job> getJobById(String jobId) {
        return Mono.fromFuture(jobs.get(jobId));
    }

    private void configureMetrics() {
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "titusApi.errors"))
                .monitorValue(apiErrors);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "titusApi.numSnapshotUpdates"))
                .monitorValue(numSnapshotUpdates);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "titusApi.numTaskUpdates"))
                .monitorValue(numTaskUpdates);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "titusApi.numJobUpdates"))
                .monitorValue(numJobUpdates);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "titusApi.numMissingJobUpdate"))
                .monitorValue(numMissingJobUpdate);
    }


    private <STUB extends AbstractStub<STUB>> STUB attachCallerId(STUB serviceStub, String callerId) {
        Metadata metadata = new Metadata();
        metadata.put(CALLER_ID_KEY, callerId);
        return serviceStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    private AsyncLoadingCache<String, Job> buildCacheForJobs() {
        return Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .buildAsync(new AsyncCacheLoader<String, Job>() {
                    @Nonnull
                    @Override
                    public CompletableFuture<Job> asyncLoad(@Nonnull String jobId, @Nonnull Executor executor) {
                        ListenableFuture<Job> jobFuture = jobManagementServiceFutureStub.findJob(JobId.newBuilder().setId(jobId).build());
                        return toCompletableFuture(jobFuture, executor);
                    }
                });
    }

    private <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> listenableFuture, Executor executor) {
        CompletableFuture<T> completableFuture = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                listenableFuture.cancel(mayInterruptIfRunning);
                return super.cancel(mayInterruptIfRunning);
            }
        };

        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable T result) {
                completableFuture.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completableFuture.completeExceptionally(t);
            }
        }, executor);
        return completableFuture;
    }
}
