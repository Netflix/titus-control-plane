/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.util.jackson.CommonObjectMappers;
import com.netflix.titus.ext.elasticsearch.model.BulkEsIndexResp;
import com.netflix.titus.ext.elasticsearch.model.EsIndexResp;
import com.netflix.titus.ext.elasticsearch.model.EsRespCount;
import com.netflix.titus.ext.elasticsearch.model.EsRespSrc;
import com.netflix.titus.ext.elasticsearch.model.IndexHeader;
import com.netflix.titus.ext.elasticsearch.model.IndexHeaderLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

public class DefaultEsClient<T extends EsDoc> implements EsClient<T> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultEsClient.class);
    private final WebClient client;

    public DefaultEsClient(EsWebClientFactory esWebClientFactory) {
        client = esWebClientFactory.buildWebClient();
    }

    @Override
    public Mono<EsIndexResp> indexDocument(T document, String indexName, String documentType) {
        return client.post()
                .uri(String.format("/%s/%s/%s", indexName, documentType, document.getId()))
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(document))
                .retrieve()
                .bodyToMono(EsIndexResp.class);
    }

    @Override
    public Mono<BulkEsIndexResp> bulkIndexDocuments(List<T> taskDocuments, String index, String type) {
        return client.post()
                .uri("/_bulk")
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(buildBulkIndexPayload(taskDocuments, index, type)))
                .retrieve()
                .bodyToMono(BulkEsIndexResp.class);
    }

    @Override
    public Mono<EsRespSrc<T>> findDocumentById(String id, String index, String type,
                                               ParameterizedTypeReference<EsRespSrc<T>> responseTypeRef) {
        return client.get()
                .uri(uriBuilder -> uriBuilder.path(String.format("%s/%s/%s", index, type, id)).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(responseTypeRef);
    }

    @Override
    public Mono<EsRespCount> getTotalDocumentCount(String index, String type) {
        return client.get()
                .uri(uriBuilder -> uriBuilder.path(String.format("%s/%s/_count", index, type)).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<EsRespCount>() {
                });
    }

    @VisibleForTesting
    String buildBulkIndexPayload(List<T> tasks, String index, String type) {
        StringBuilder sb = new StringBuilder();

        final ObjectMapper mapper = CommonObjectMappers.jacksonDefaultMapper();

        tasks.forEach(taskDocument -> {
            final IndexHeader indexHeader = new IndexHeader(index, type, taskDocument.getId());
            final IndexHeaderLine indexHeaderLine = new IndexHeaderLine(indexHeader);
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
}
