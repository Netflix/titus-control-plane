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

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.MetricConstants;
import io.kubernetes.client.informer.SharedIndexInformer;

class KubeInformerMetrics<ApiType> {

    private static final String METRICS_ROOT = MetricConstants.METRIC_KUBERNETES + "kubeClient.";

    private static final String METRICS_INFORMER = METRICS_ROOT + "informer";

    private static final String METRICS_INFORMER_SYNCED = METRICS_ROOT + "informerSynced";

    private final Id sizeGaugeId;
    private final Id syncedGaugeId;

    private final TitusRuntime titusRuntime;

    public KubeInformerMetrics(String type,
                               SharedIndexInformer<ApiType> informer,
                               TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.sizeGaugeId = titusRuntime.getRegistry().createId(METRICS_INFORMER, "type", type);
        this.syncedGaugeId = titusRuntime.getRegistry().createId(METRICS_INFORMER_SYNCED, "type", type);

        PolledMeter.using(titusRuntime.getRegistry())
                .withId(sizeGaugeId)
                .monitorValue(informer, i -> i.getIndexer().list().size());
        PolledMeter.using(titusRuntime.getRegistry())
                .withId(syncedGaugeId)
                .monitorValue(informer, i -> i.hasSynced() ? 1 : 0);
    }

    void shutdown() {
        PolledMeter.remove(titusRuntime.getRegistry(), sizeGaugeId);
        PolledMeter.remove(titusRuntime.getRegistry(), syncedGaugeId);
    }
}
