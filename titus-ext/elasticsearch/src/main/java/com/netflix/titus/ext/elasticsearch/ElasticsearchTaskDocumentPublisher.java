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

package com.netflix.titus.ext.elasticsearch;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.MetricConstants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import static com.netflix.titus.ext.elasticsearch.ElasticsearchModule.TASK_DOCUMENT_CONTEXT;

@Singleton
public class ElasticsearchTaskDocumentPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTaskDocumentPublisher.class);

    private static final int TIME_TO_BUFFER_MS = 10_000;
    private static final int COUNT_TO_BUFFER = 1_000;
    private static final String DEFAULT_DOC_TYPE = "default";

    private final ElasticsearchConfiguration configuration;
    private final V3JobOperations v3JobOperations;
    private final Client client;
    private final Map<String, String> taskDocumentContext;
    private final TitusRuntime titusRuntime;
    private final ObjectMapper objectMapper;
    private final SimpleDateFormat indexDateFormat;
    private final SimpleDateFormat taskDateFormat;
    private final AtomicInteger docsToBePublished = new AtomicInteger(0);
    private final AtomicInteger docsPublished = new AtomicInteger(0);
    private final AtomicInteger errorJsonConversion = new AtomicInteger(0);
    private final AtomicInteger errorEsClient = new AtomicInteger(0);
    private final AtomicInteger errorInPublishing = new AtomicInteger(0);
    private final AtomicInteger isAlive = new AtomicInteger(0);

    @Inject
    public ElasticsearchTaskDocumentPublisher(ElasticsearchConfiguration configuration,
                                              V3JobOperations v3JobOperations,
                                              Client client,
                                              @Named(TASK_DOCUMENT_CONTEXT) Map<String, String> taskDocumentContext,
                                              TitusRuntime titusRuntime,
                                              Registry registry) {
        this.configuration = configuration;
        this.v3JobOperations = v3JobOperations;
        this.client = client;
        this.taskDocumentContext = taskDocumentContext;
        this.titusRuntime = titusRuntime;

        this.objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.indexDateFormat = new SimpleDateFormat(configuration.getTaskDocumentEsIndexDateSuffixPattern());
        this.indexDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.taskDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        this.taskDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        PolledMeter.using(registry).withName(MetricConstants.METRIC_ES_PUBLISHER + "docsToBePublished").monitorValue(docsToBePublished);
        PolledMeter.using(registry).withName(MetricConstants.METRIC_ES_PUBLISHER + "docsPublished").monitorValue(docsPublished);
        PolledMeter.using(registry).withName(MetricConstants.METRIC_ES_PUBLISHER + "errorJsonConversion").monitorValue(errorJsonConversion);
        PolledMeter.using(registry).withName(MetricConstants.METRIC_ES_PUBLISHER + "errorEsClient").monitorValue(errorEsClient);
        PolledMeter.using(registry).withName(MetricConstants.METRIC_ES_PUBLISHER + "error").monitorValue(errorInPublishing);
        PolledMeter.using(registry).withName(MetricConstants.METRIC_ES_PUBLISHER + "isAlive").monitorValue(isAlive);
    }

    @Activator
    public void enterActiveMode() {
        logger.info("Starting the task streams to publish task documents to elasticsearch");
        v3TasksStream()
                .subscribe(
                        this::publishTaskDocuments,
                        e -> {
                            errorInPublishing.incrementAndGet();
                            isAlive.set(0);
                            logger.error("Unable to publish task documents to elasticsearch: ", e);
                        },
                        () -> {
                            isAlive.set(0);
                            logger.info("Finished publishing task documents to elasticsearch");
                        }
                );
    }

    private Observable<List<TaskDocument>> v3TasksStream() {
        Observable<Optional<TaskDocument>> optionalTaskDocuments = v3JobOperations.observeJobs()
                .filter(event -> event instanceof TaskUpdateEvent)
                .cast(TaskUpdateEvent.class)
                .map(event -> {
                    Task task = event.getCurrentTask();
                    Job<?> job = event.getCurrentJob();
                    TaskDocument taskDocument = TaskDocument.fromV3Task(task, job, taskDateFormat, taskDocumentContext);
                    return Optional.of(taskDocument);
                });
        final Observable<List<TaskDocument>> taskDocumentsObservable = ObservableExt.fromOptionalObservable(optionalTaskDocuments)
                .buffer(TIME_TO_BUFFER_MS, TimeUnit.MILLISECONDS, COUNT_TO_BUFFER).observeOn(Schedulers.io());

        return titusRuntime.persistentStream(taskDocumentsObservable);
    }

    private void publishTaskDocuments(List<TaskDocument> taskDocuments) {
        try {
            isAlive.set(1);
            if (configuration.isEnabled() && !taskDocuments.isEmpty()) {
                Map<String, String> documentsToIndex = new HashMap<>();
                for (TaskDocument taskDocument : taskDocuments) {
                    String documentId = taskDocument.getInstanceId();
                    try {
                        String documentAsJson = objectMapper.writeValueAsString(taskDocument);
                        documentsToIndex.put(documentId, documentAsJson);
                    } catch (Exception e) {
                        errorJsonConversion.incrementAndGet();
                        errorInPublishing.incrementAndGet();
                        logger.warn("Unable to convert document with id: {} to json with error: ", documentId, e);
                    }
                }

                if (!documentsToIndex.isEmpty()) {
                    logger.info("Attempting to index {} task documents to elasticsearch", documentsToIndex.size());
                    docsToBePublished.addAndGet(documentsToIndex.size());
                    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                    for (Map.Entry<String, String> entry : documentsToIndex.entrySet()) {
                        String documentId = entry.getKey();
                        String documentJson = entry.getValue();
                        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(getEsIndexName(), DEFAULT_DOC_TYPE, documentId)
                                .setSource(documentJson);
                        bulkRequestBuilder.add(indexRequestBuilder);
                        logger.debug("Indexing task document with id: {} and json: {}", documentId, documentJson);
                    }

                    bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse bulkItemResponses) {
                            BulkItemResponse[] items = bulkItemResponses.getItems();
                            if (items != null) {
                                int successCount = 0;
                                for (BulkItemResponse bulkItemResponse : items) {
                                    if (!bulkItemResponse.isFailed()) {
                                        String documentJson = documentsToIndex.get(bulkItemResponse.getId());
                                        logger.debug("Successfully indexed task document with id: {} and json: {}", bulkItemResponse.getId(), documentJson);
                                        successCount++;
                                    }
                                }
                                logger.info("Successfully indexed {} out of {} task documents", successCount, items.length);
                                docsPublished.addAndGet(successCount);
                            }
                            if (bulkItemResponses.hasFailures()) {
                                errorEsClient.incrementAndGet();
                                errorInPublishing.incrementAndGet();
                                logger.error(bulkItemResponses.buildFailureMessage());
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("Error in indexing task documents with error: ", e);
                            errorEsClient.incrementAndGet();
                            errorInPublishing.incrementAndGet();
                        }
                    });
                }
            }
        } catch (Exception e) {
            errorInPublishing.incrementAndGet();
            logger.error("Exception in ElasticsearchTaskDocumentPublisher - ", e);
        }
    }

    private String getEsIndexName() {
        return configuration.getTaskDocumentEsIndexName() + indexDateFormat.format(new Date());
    }
}
