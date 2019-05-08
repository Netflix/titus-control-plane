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
package com.netflix.titus.supplementary.taskspublisher.es;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.netflix.spectator.api.Functions;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.supplementary.taskspublisher.TaskDocument;
import com.netflix.titus.supplementary.taskspublisher.TaskEventsGenerator;
import com.netflix.titus.supplementary.taskspublisher.TaskPublisherRetryUtil;
import com.netflix.titus.supplementary.taskspublisher.TasksPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;

public class EsPublisher implements TasksPublisher {
    private static final Logger logger = LoggerFactory.getLogger(EsPublisher.class);
    private TaskEventsGenerator taskEventsGenerator;
    private final EsClient esClient;
    private Registry registry;

    private AtomicInteger numErrors = new AtomicInteger(0);
    private AtomicInteger numTasksUpdated = new AtomicInteger(0);
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
                                .retryWhen(TaskPublisherRetryUtil.buildRetryHandler(
                                        TaskPublisherRetryUtil.INITIAL_RETRY_DELAY_MS,
                                        TaskPublisherRetryUtil.MAX_RETRY_DELAY_MS, 3)))
                .doOnError(e -> {
                    logger.error("Error in indexing documents (Retrying) : ", e);
                    numErrors.incrementAndGet();
                })
                .retryWhen(TaskPublisherRetryUtil.buildRetryHandler(TaskPublisherRetryUtil.INITIAL_RETRY_DELAY_MS,
                        TaskPublisherRetryUtil.MAX_RETRY_DELAY_MS, -1))
                .subscribe(bulkIndexResp -> {
                            logger.info("Received bulk response for {} items", bulkIndexResp.items.size());
                            lastPublishedTimestamp.set(registry.clock().wallTime());
                            bulkIndexResp.items.forEach(bulkEsIndexRespItem -> {
                                String indexedItemId = bulkEsIndexRespItem.index._id;
                                logger.info("Index result <{}> for task ID {}", bulkEsIndexRespItem.index.result, indexedItemId);
                                numTasksUpdated.incrementAndGet();
                            });
                        },
                        e -> logger.error("Error in indexing documents ", e));
        taskEventsSourceConnection = taskEvents.connect();
    }

    @Override
    public int getNumErrorsInPublishing() {
        return numErrors.get();
    }

    @Override
    public int getNumTasksPublished() {
        return numTasksUpdated.get();
    }

    private void configureMetrics() {
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "errors"))
                .monitorValue(numErrors);
        PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "numTasksUpdated"))
                .monitorValue(numTasksUpdated);

        lastPublishedTimestamp = PolledMeter.using(registry)
                .withId(registry.createId(EsTaskPublisherMetrics.METRIC_ES_PUBLISHER + "lastPublishedTimestamp"))
                .monitorValue(new AtomicLong(registry.clock().wallTime()), Functions.AGE);
    }
}
