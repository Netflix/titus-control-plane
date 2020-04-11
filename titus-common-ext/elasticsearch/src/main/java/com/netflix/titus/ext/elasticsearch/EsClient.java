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

import com.netflix.titus.ext.elasticsearch.model.BulkEsIndexResp;
import com.netflix.titus.ext.elasticsearch.model.EsIndexResp;
import com.netflix.titus.ext.elasticsearch.model.EsRespCount;
import com.netflix.titus.ext.elasticsearch.model.EsRespSrc;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Mono;

public interface EsClient<T extends EsDoc> {
    Mono<EsIndexResp> indexDocument(T document, String indexName, String documentType);

    Mono<BulkEsIndexResp> bulkIndexDocuments(List<T> documents, String index, String type);

    Mono<EsRespSrc<T>> findDocumentById(String id, String index, String type,
                                        ParameterizedTypeReference<EsRespSrc<T>> responseTypeRef);

    Mono<EsRespCount> getTotalDocumentCount(String index, String type);
}
