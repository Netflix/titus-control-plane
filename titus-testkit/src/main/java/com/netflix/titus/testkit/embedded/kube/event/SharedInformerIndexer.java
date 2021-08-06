/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.kube.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.cache.Indexer;

class SharedInformerIndexer<ApiType extends KubernetesObject> implements Indexer<ApiType> {

    private final ConcurrentMap<String, ApiType> cache;

    SharedInformerIndexer(ConcurrentMap<String, ApiType> cache) {
        this.cache = cache;
    }

    @Override
    public List<ApiType> index(String indexName, ApiType obj) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public List<String> indexKeys(String indexName, String indexKey) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public List<ApiType> byIndex(String indexName, String indexKey) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public Map<String, Function<ApiType, List<String>>> getIndexers() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void addIndexers(Map<String, Function<ApiType, List<String>>> indexers) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void add(ApiType obj) {
    }

    @Override
    public void update(ApiType obj) {
    }

    @Override
    public void delete(ApiType obj) {
    }

    @Override
    public void replace(List<ApiType> list, String resourceVersion) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void resync() {
    }

    @Override
    public List<String> listKeys() {
        return new ArrayList<>(cache.keySet());
    }

    @Override
    public Object get(ApiType obj) {
        return null;
    }

    @Override
    public ApiType getByKey(String key) {
        return null;
    }

    @Override
    public List<ApiType> list() {
        return new ArrayList<>(cache.values());
    }
}
