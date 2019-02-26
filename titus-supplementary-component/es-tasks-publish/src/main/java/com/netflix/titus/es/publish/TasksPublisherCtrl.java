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
package com.netflix.titus.es.publish;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.PostConstruct;
import javax.inject.Named;

import com.netflix.spectator.api.Functions;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.ext.elasticsearch.TaskDocument;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.es.publish.TitusClientComponent.TASK_DOCUMENT_CONTEXT;

@Component
public class TasksPublisherCtrl {
    private static final Logger logger = LoggerFactory.getLogger(TasksPublisherCtrl.class);
    private final EsClient esClient;
    private Map<String, String> taskDocumentBaseContext;
    private Registry registry;
    private TitusClient titusClient;

    private AtomicInteger numErrors = new AtomicInteger(0);
    private AtomicInteger numIndexUpdated = new AtomicInteger(0);
    private AtomicInteger numTasksUpdated = new AtomicInteger(0);
    private Set<String> uniqueTasksIdUpdated = new ConcurrentHashSet<>();
    private AtomicLong lastPublishedTimestamp;

    private static final long INITIAL_RETRY_DELAY_MS = 500;
    private static final long MAX_RETRY_DELAY_MS = 2_000;


    @Autowired
    public TasksPublisherCtrl(EsClient esClient,
                              TitusClient titusClient,
                              @Named(TASK_DOCUMENT_CONTEXT) Map<String, String> taskDocumentBaseContext,
                              Registry registry) {
        this.esClient = esClient;
        this.titusClient = titusClient;
        this.taskDocumentBaseContext = taskDocumentBaseContext;
        this.registry = registry;
        configureMetrics();
    }


    public AtomicInteger getNumErrors() {
        return numErrors;
    }

    public AtomicInteger getNumIndexUpdated() {
        return numIndexUpdated;
    }

    public AtomicInteger getNumTasksUpdated() {
        return numTasksUpdated;
    }

    @PostConstruct
    public void start() {
        titusClient.getTaskUpdates()
                .publishOn(Schedulers.elastic())
                .map(task -> {
                    final Mono<Job> jobById = titusClient.getJobById(task.getJobId());
                    return Pair.of(task, jobById);
                })
                .flatMap(taskMonoPair -> {
                    final Task task = taskMonoPair.getLeft();
                    return taskMonoPair.getRight()
                            .map(job -> {
                                final com.netflix.titus.api.jobmanager.model.job.Job coreJob = V3GrpcModelConverters.toCoreJob(job);
                                final com.netflix.titus.api.jobmanager.model.job.Task coreTask = V3GrpcModelConverters.toCoreTask(coreJob, task);
                                return TaskDocument.fromV3Task(coreTask, coreJob, ElasticSearchUtils.dateFormat, buildTaskContext(task));
                            }).flux();
                })
                .bufferTimeout(100, Duration.ofSeconds(5))
                .flatMap(taskDocuments -> esClient.bulkIndexTaskDocument(taskDocuments)
                        .retryWhen(buildLimitedRetryHandler()))
                .doOnError(e -> {
                    logger.error("Error in indexing documents (Retrying) : ", e);
                    numErrors.incrementAndGet();
                })
                .retryWhen(buildUnlimitedRetryHandler())
                .subscribe(bulkIndexResp -> {
                            logger.info("Received bulk response for {} items", bulkIndexResp.items.size());
                            lastPublishedTimestamp.set(registry.clock().wallTime());
                            bulkIndexResp.items.forEach(bulkEsIndexRespItem -> {
                                String indexedItemId = bulkEsIndexRespItem.index._id;
                                logger.info("Index result <{}> for task ID {}", bulkEsIndexRespItem.index.result, indexedItemId);
                                numIndexUpdated.incrementAndGet();
                                if (!uniqueTasksIdUpdated.contains(indexedItemId)) {
                                    uniqueTasksIdUpdated.add(indexedItemId);
                                    numTasksUpdated.incrementAndGet();
                                }
                            });
                        },
                        e -> logger.error("Error in indexing documents ", e));
    }

    private Map<String, String> buildTaskContext(Task task) {
        String stack = "";
        if (task.getTaskContextMap().containsKey(JobAttributes.JOB_ATTRIBUTES_CELL)) {
            stack = task.getTaskContextMap().get(JobAttributes.JOB_ATTRIBUTES_CELL);
        }
        final HashMap<String, String> taskContext = new HashMap<>(taskDocumentBaseContext);
        taskContext.put("stack", stack);
        return taskContext;
    }

    private Function<Flux<Throwable>, Publisher<?>> buildUnlimitedRetryHandler() {
        return RetryHandlerBuilder.retryHandler()
                .withUnlimitedRetries()
                .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                .withReactorScheduler(Schedulers.elastic())
                .buildReactorExponentialBackoff();
    }

    private Function<Flux<Throwable>, Publisher<?>> buildLimitedRetryHandler() {
        return RetryHandlerBuilder.retryHandler()
                .withRetryCount(3)
                .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                .withReactorScheduler(Schedulers.elastic())
                .buildReactorExponentialBackoff();
    }

    private void configureMetrics() {
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "errors"))
                .monitorValue(numErrors);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "numIndexUpdated"))
                .monitorValue(numIndexUpdated);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "numTasksUpdated"))
                .monitorValue(numTasksUpdated);

        lastPublishedTimestamp = PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "lastPublishedTimestamp"))
                .monitorValue(new AtomicLong(registry.clock().wallTime()), Functions.AGE);
    }


}
