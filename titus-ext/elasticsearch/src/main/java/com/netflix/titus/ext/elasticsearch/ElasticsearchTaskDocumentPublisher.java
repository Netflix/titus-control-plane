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
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.event.TaskStateChangeEvent;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
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
    private final RxEventBus rxEventBus;
    private final Client client;
    private final Map<String, String> taskDocumentContext;
    private final TitusRuntime titusRuntime;
    private final ObjectMapper objectMapper;
    private final SimpleDateFormat indexDateFormat;
    private final SimpleDateFormat taskDateFormat;

    @Inject
    public ElasticsearchTaskDocumentPublisher(ElasticsearchConfiguration configuration,
                                              V3JobOperations v3JobOperations,
                                              RxEventBus rxEventBus,
                                              Client client,
                                              @Named(TASK_DOCUMENT_CONTEXT) Map<String, String> taskDocumentContext,
                                              TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.v3JobOperations = v3JobOperations;
        this.rxEventBus = rxEventBus;
        this.client = client;
        this.taskDocumentContext = taskDocumentContext;
        this.titusRuntime = titusRuntime;

        this.objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.indexDateFormat = new SimpleDateFormat(configuration.getTaskDocumentEsIndexDateSuffixPattern());
        this.indexDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.taskDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        this.taskDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Activator
    public void enterActiveMode() {
        logger.info("Starting the task streams to publish task documents to elasticsearch");
        Observable.merge(v2TasksStream(), v3TasksStream()).buffer(TIME_TO_BUFFER_MS, TimeUnit.MILLISECONDS, COUNT_TO_BUFFER)
                .observeOn(Schedulers.io())
                .subscribe(
                        this::publishTaskDocuments,
                        e -> logger.error("Unable to publish task documents to elasticsearch: ", e),
                        () -> logger.info("Finished publishing task documents to elasticsearch")
                );
    }

    private Observable<TaskDocument> v2TasksStream() {
        Observable<Optional<TaskDocument>> optionalTaskDocuments = rxEventBus.listen(getClass().getSimpleName(), TaskStateChangeEvent.class)
                .filter(taskStateChangeEvent -> taskStateChangeEvent.getSource() instanceof Pair)
                .map(taskStateChangeEvent -> {
                    try {
                        Pair<V2JobMetadata, V2WorkerMetadata> jobAndTaskPair = (Pair<V2JobMetadata, V2WorkerMetadata>) taskStateChangeEvent.getSource();
                        V2JobMetadata job = jobAndTaskPair.getLeft();
                        V2WorkerMetadata task = jobAndTaskPair.getRight();
                        if (job != null && task != null) {
                            TitusJobSpec titusJobSpec = TitusJobSpec.getSpec(job);
                            TaskDocument taskDocument = TaskDocument.fromV2Task(task, titusJobSpec, taskDateFormat, taskDocumentContext);
                            return Optional.of(taskDocument);
                        }
                    } catch (Exception e) {
                        logger.error("Unable to convert task: {} to task document with error: ", taskStateChangeEvent.getTaskId(), e);
                    }
                    return Optional.empty();
                });
        return titusRuntime.persistentStream(ObservableExt.fromOptionalObservable(optionalTaskDocuments));
    }

    private Observable<TaskDocument> v3TasksStream() {
        Observable<Optional<TaskDocument>> optionalTaskDocuments = v3JobOperations.observeJobs()
                .filter(event -> event instanceof TaskUpdateEvent)
                .cast(TaskUpdateEvent.class)
                .map(event -> {
                    Task task = event.getCurrentTask();
                    Job<?> job = event.getCurrentJob();
                    TaskDocument taskDocument = TaskDocument.fromV3Task(task, job, taskDateFormat, taskDocumentContext);
                    return Optional.of(taskDocument);
                });
        return titusRuntime.persistentStream(ObservableExt.fromOptionalObservable(optionalTaskDocuments));
    }

    private void publishTaskDocuments(List<TaskDocument> taskDocuments) {
        if (configuration.isEnabled() && !taskDocuments.isEmpty()) {
            Map<String, String> documentsToIndex = new HashMap<>();
            for (TaskDocument taskDocument : taskDocuments) {
                String documentId = taskDocument.getInstanceId();
                try {
                    String documentAsJson = objectMapper.writeValueAsString(taskDocument);
                    documentsToIndex.put(documentId, documentAsJson);
                } catch (Exception e) {
                    logger.warn("Unable to convert document with id: {} to json with error: ", documentId, e);
                }
            }

            if (!documentsToIndex.isEmpty()) {
                logger.info("Attempting to index {} task documents to elasticsearch", documentsToIndex.size());
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
                        }
                        if (bulkItemResponses.hasFailures()) {
                            logger.error(bulkItemResponses.buildFailureMessage());
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("Error in indexing task documents with error: ", e);
                    }
                });
            }
        }
    }

    private String getEsIndexName() {
        return configuration.getTaskDocumentEsIndexName() + indexDateFormat.format(new Date());
    }
}
