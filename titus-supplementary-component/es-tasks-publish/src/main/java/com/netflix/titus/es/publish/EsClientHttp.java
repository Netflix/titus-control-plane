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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.es.publish.config.EsPublisherConfiguration;
import com.netflix.titus.ext.elasticsearch.TaskDocument;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

@Component
public class EsClientHttp implements EsClient {
    private static final Logger logger = LoggerFactory.getLogger(EsClientHttp.class);
    private static final String ES_RECORD_TYPE = "default";
    private final WebClient tasksClient;
    private SimpleDateFormat indexDateFormat;
    private EsPublisherConfiguration esPublisherConfiguration;


    @Inject
    public EsClientHttp(EsPublisherConfiguration esPublisherConfiguration) {
        this.esPublisherConfiguration = esPublisherConfiguration;
        tasksClient = WebClient.builder().clientConnector(new ReactorClientHttpConnector(buildHttpClient()))
                .baseUrl(buildEsUrl()).build();
        indexDateFormat = new SimpleDateFormat(esPublisherConfiguration.getTaskDocumentEsIndexDateSuffixPattern());
    }


    @Override
    public Mono<EsIndexResp> indexTaskDocument(TaskDocument taskDocument) {
        logger.debug("Indexing TASK {} in thread {}", taskDocument.getId(), Thread.currentThread().getName());
        return tasksClient.put()
                .uri(String.format("/%s/default/%s", buildEsIndexNameCurrent(), taskDocument.getId()))
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromObject(taskDocument))
                .retrieve()
                .bodyToMono(EsIndexResp.class);
    }

    @Override
    public Mono<BulkEsIndexResp> bulkIndexTaskDocument(List<TaskDocument> taskDocuments) {
        return tasksClient.post()
                .uri("/_bulk")
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromObject(buildBulkIndexPayload(taskDocuments, buildEsIndexNameCurrent())))
                .retrieve()
                .bodyToMono(BulkEsIndexResp.class);
    }


    @Override
    public Mono<EsRespSrc<TaskDocument>> findTaskById(String taskId) {
        return tasksClient.get()
                .uri(uriBuilder -> uriBuilder.path(String.format("%s/default/%s", buildEsIndexNameCurrent(), taskId)).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<EsRespSrc<TaskDocument>>() {
                });
    }

    @VisibleForTesting
    String buildEsUrl() {
        return String.format("http://%s:%s", esPublisherConfiguration.getGetTaskDocumentEsHostName(),
                esPublisherConfiguration.getGetTaskDocumentEsPort());
    }

    @VisibleForTesting
    String buildEsIndexNameCurrent() {
        return String.format("%s%s",
                esPublisherConfiguration.getTaskDocumentEsIndexName(),
                indexDateFormat.format(new Date()));

    }

    @VisibleForTesting
    String buildBulkIndexPayload(List<TaskDocument> tasks, String esIndexName) {
        StringBuilder sb = new StringBuilder();

        final ObjectMapper mapper = ObjectMappers.jacksonDefaultMapper();

        tasks.forEach(taskDocument -> {
            final IndexHeader indexHeader = new IndexHeader();
            indexHeader.set_id(taskDocument.getId());
            indexHeader.set_index(esIndexName);
            indexHeader.set_type(ES_RECORD_TYPE);
            final IndexHeaderLine indexHeaderLine = new IndexHeaderLine();
            indexHeaderLine.setIndex(indexHeader);
            try {
                final String indexLine = mapper.writeValueAsString(indexHeaderLine);
                sb.append(indexLine);
                sb.append("\n");
                final String fieldsLine = mapper.writeValueAsString(taskDocument);
                sb.append(fieldsLine);
                sb.append("\n");
            } catch (JsonProcessingException e) {
                logger.error("Exception in transforming taskDocument into JSON ", e);
            }
        });
        return sb.toString();
    }

    private HttpClient buildHttpClient() {
        return HttpClient.create().tcpConfiguration(tcpClient -> {
            TcpClient tcpClientWithConnectionTimeout = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
            return tcpClientWithConnectionTimeout.doOnConnected(connection -> {
                connection.addHandlerLast(new ReadTimeoutHandler(20));
                connection.addHandlerLast(new WriteTimeoutHandler(20));
            });
        });
    }

}
