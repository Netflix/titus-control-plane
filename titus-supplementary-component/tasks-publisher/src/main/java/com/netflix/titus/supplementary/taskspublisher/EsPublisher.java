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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.netflix.spectator.api.Functions;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.ext.elasticsearch.TaskDocument;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class EsPublisher implements TasksPublisher {
    private static final Logger logger = LoggerFactory.getLogger(EsPublisher.class);
    private TaskEventsGenerator taskEventsGenerator;
    private final EsClient esClient;
    private Registry registry;
    private static final long INITIAL_RETRY_DELAY_MS = 500;
    private static final long MAX_RETRY_DELAY_MS = 2_000;
    private AtomicInteger numErrors = new AtomicInteger(0);
    private AtomicInteger numIndexUpdated = new AtomicInteger(0);
    private AtomicInteger numTasksUpdated = new AtomicInteger(0);
    private Set<String> uniqueTasksIdUpdated = new ConcurrentHashSet<>();
    private AtomicLong lastPublishedTimestamp;
    private Disposable subscription;
    private Disposable taskEventsSourceConnection;


    public EsPublisher(TaskEventsGenerator taskEventsGenerator, EsClient esClient, Registry registry) {
        this.taskEventsGenerator = taskEventsGenerator;
        this.esClient = esClient;
        this.registry = registry;
        configureMetrics();
    }


    @PreDestroy
    public void stop() {
        ReactorExt.safeDispose(subscription, taskEventsSourceConnection);
    }

    @PostConstruct
    public void start() {
        ConnectableFlux<TaskDocument> taskEvents = taskEventsGenerator.getTaskEvents();
        subscription = taskEvents.bufferTimeout(100, Duration.ofSeconds(5))
                .flatMap(taskDocuments ->
                        esClient.bulkIndexTaskDocument(taskDocuments)
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
        taskEventsSourceConnection = taskEvents.connect();
    }

    @Override
    public int getNumErrorsInPublishing() {
        return numErrors.get();
    }

    public int getNumIndexUpdated() {
        return numIndexUpdated.get();
    }

    @Override
    public int getNumTasksPublished() {
        return numTasksUpdated.get();
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


    private Function<Flux<Throwable>, Publisher<?>> buildUnlimitedRetryHandler() {
        return RetryHandlerBuilder.retryHandler()
                .withUnlimitedRetries()
                .withDelay(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                .withReactorScheduler(Schedulers.elastic())
                .buildReactorExponentialBackoff();
    }

}
