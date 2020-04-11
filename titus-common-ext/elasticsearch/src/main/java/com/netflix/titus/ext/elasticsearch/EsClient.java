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

import com.netflix.titus.ext.elasticsearch.model.EsRespCount;
import com.netflix.titus.ext.elasticsearch.model.EsRespSrc;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Mono;

public interface EsClient<T extends EsDoc> {
    class EsIndexResp {
        boolean created;
        String result;
        String _id;

        public boolean isCreated() {
            return created;
        }

        public void setCreated(boolean created) {
            this.created = created;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }
    }

    class BulkEsIndexRespItem {
        EsIndexResp index;

        public EsIndexResp getIndex() {
            return index;
        }

        public void setIndex(EsIndexResp index) {
            this.index = index;
        }
    }

    class BulkEsIndexResp {
        List<BulkEsIndexRespItem> items;

        public List<BulkEsIndexRespItem> getItems() {
            return items;
        }

        public void setItems(List<BulkEsIndexRespItem> items) {
            this.items = items;
        }
    }

    class IndexHeaderLine {
        private IndexHeader index;

        public IndexHeader getIndex() {
            return index;
        }

        public void setIndex(IndexHeader index) {
            this.index = index;
        }
    }

    class IndexHeader {
        private String _index;
        private String _type;
        private String _id;

        public String get_index() {
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }
    }


    Mono<EsIndexResp> indexDocument(T document, String indexName, String documentType);

    Mono<BulkEsIndexResp> bulkIndexDocument(List<T> documents, String index, String type);

    Mono<EsRespSrc<T>> findDocumentById(String id, String index, String type,
                                        ParameterizedTypeReference<EsRespSrc<T>> responseTypeRef);

    Mono<EsRespCount> getTotalDocumentCount(String index, String type);
}
