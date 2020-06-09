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

package com.netflix.titus.master.mesos.kubeapiserver.client;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeApiServerIntegrator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.KubeFitAction;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;

public class FitSharedIndexInformer<T> implements SharedIndexInformer<T> {

    private final SharedIndexInformer<T> source;

    private final FitInjection fitKubeInjection;

    public FitSharedIndexInformer(String id,
                                  SharedIndexInformer<T> source,
                                  TitusRuntime titusRuntime) {
        this.source = source;

        FitFramework fit = titusRuntime.getFitFramework();
        this.fitKubeInjection = fit.newFitInjectionBuilder(id)
                .withDescription("SharedIndexInformer FIT injection for " + id)
                .build();
        fit.getRootComponent().getChild(DirectKubeApiServerIntegrator.COMPONENT).addInjection(fitKubeInjection);
    }

    @Override
    public void addIndexers(Map<String, Function<T, List<String>>> indexers) {
        source.addIndexers(indexers);
    }

    @Override
    public Indexer<T> getIndexer() {
        return source.getIndexer();
    }

    @Override
    public void addEventHandler(ResourceEventHandler<T> handler) {
        source.addEventHandler(handler);
    }

    @Override
    public void addEventHandlerWithResyncPeriod(ResourceEventHandler<T> handler, long resyncPeriod) {
        source.addEventHandlerWithResyncPeriod(handler, resyncPeriod);
    }

    @Override
    public void run() {
        source.run();
    }

    @Override
    public void stop() {
        source.stop();
    }

    @Override
    public boolean hasSynced() {
        try {
            fitKubeInjection.beforeImmediate(KubeFitAction.ErrorKind.INFORMER_NOT_SYNCED.name());
        } catch (Exception e) {
            return false;
        }
        return source.hasSynced();
    }

    @Override
    public String lastSyncResourceVersion() {
        return source.lastSyncResourceVersion();
    }
}
