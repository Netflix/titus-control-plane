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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class SharedInformerStub<ApiType extends KubernetesObject> implements SharedIndexInformer<ApiType> {

    private static final Logger logger = LoggerFactory.getLogger(SharedInformerStub.class);

    private final Flux<EmbeddedKubeEvent<ApiType>> changes;
    private final SharedInformerIndexer<ApiType> indexer;
    private final ConcurrentMap<String, ApiType> cache = new ConcurrentHashMap<>();

    public SharedInformerStub(List<ApiType> snapshot, Flux<EmbeddedKubeEvent<ApiType>> changes) {
        this.changes = changes;
        this.indexer = new SharedInformerIndexer<>(cache);
        snapshot.forEach(v -> cache.put(v.getMetadata().getName(), v));
        changes.subscribe(
                next -> {
                    ApiType current = next.getCurrent();
                    String name = current.getMetadata().getName();
                    switch (next.getKind()) {
                        case ADDED:
                            cache.put(name, current);
                            indexer.add(current);
                            break;
                        case UPDATED:
                            cache.put(name, current);
                            indexer.update(current);
                            break;
                        case DELETED:
                            cache.remove(name);
                            indexer.delete(current);
                            break;
                    }
                },
                e -> logger.error("Shared informer cache updater error", e),
                () -> logger.info("Shared informer cache updater completed")
        );
    }

    @Override
    public void addIndexers(Map<String, Function<ApiType, List<String>>> indexers) {
    }

    @Override
    public Indexer<ApiType> getIndexer() {
        return indexer;
    }

    @Override
    public void addEventHandler(ResourceEventHandler<ApiType> handler) {
        addEventHandlerWithResyncPeriod(handler, 0);
    }

    @Override
    public void addEventHandlerWithResyncPeriod(ResourceEventHandler<ApiType> handler, long resyncPeriod) {
        cache.forEach((id, value) -> handler.onAdd(value));
        changes.subscribe(
                next -> {
                    switch (next.getKind()) {
                        case ADDED:
                            handler.onAdd(next.getCurrent());
                            break;
                        case UPDATED:
                            handler.onUpdate(next.getPrevious(), next.getCurrent());
                            break;
                        case DELETED:
                            handler.onDelete(next.getCurrent(), false);
                            break;
                    }
                },
                e -> logger.error("SharedInformer error", e),
                () -> logger.info("SharedInformer completed")
        );
    }

    @Override
    public void run() {
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean hasSynced() {
        return true;
    }

    @Override
    public String lastSyncResourceVersion() {
        return "1";
    }
}
