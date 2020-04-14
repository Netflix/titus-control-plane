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
package com.netflix.titus.ext.elasticsearch.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Elastic search data model as defined by REST API documentation
 * https://www.elastic.co/guide/en/elasticsearch/reference/master/rest-apis.html
 */
public class BulkEsIndexResp {
    private final List<BulkEsIndexRespItem> items;

    @JsonCreator
    public BulkEsIndexResp(@JsonProperty("items") List<BulkEsIndexRespItem> items) {
        this.items = items;
    }

    public List<BulkEsIndexRespItem> getItems() {
        return items;
    }
}
