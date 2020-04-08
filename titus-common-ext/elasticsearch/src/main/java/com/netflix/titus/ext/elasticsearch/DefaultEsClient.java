package com.netflix.titus.ext.elasticsearch;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DefaultEsClient<T> implements EsClient<T> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultEsClient.class);

    @Override
    public Mono<EsIndexResp> indexTaskDocument(T taskDocument) {
        return Mono.empty();
    }

    @Override
    public Mono<BulkEsIndexResp> bulkIndexTaskDocument(List<T> taskDocuments) {
        return Mono.empty();
    }

    @Override
    public Mono<EsRespSrc<T>> findTaskById(String taskId) {
        return Mono.empty();
    }
}
