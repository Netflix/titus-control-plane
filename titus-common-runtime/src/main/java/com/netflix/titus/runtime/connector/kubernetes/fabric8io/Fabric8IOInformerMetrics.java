/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

class Fabric8IOInformerMetrics<T> {

    private static final String ROOT = "titus.kubeClient.fabric8io.";

    private final TitusRuntime titusRuntime;

    private final Id syncedId;
    private final Id watchingId;
    private final Id resourceVersionId;
    private final Id sizeId;

    Fabric8IOInformerMetrics(String name, SharedIndexInformer<T> informer, TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.syncedId = titusRuntime.getRegistry().createId(ROOT + "synced").withTag("informer", name);
        this.watchingId = titusRuntime.getRegistry().createId(ROOT + "watching").withTag("informer", name);
        this.resourceVersionId = titusRuntime.getRegistry().createId(ROOT + "resourceVersion").withTag("informer", name);
        this.sizeId = titusRuntime.getRegistry().createId(ROOT + "size").withTag("informer", name);
        PolledMeter.using(titusRuntime.getRegistry())
                .withId(syncedId)
                .monitorValue(informer, i -> i.hasSynced() ? 1 : 0);
        PolledMeter.using(titusRuntime.getRegistry())
                .withId(watchingId)
                .monitorValue(informer, i -> i.isWatching() ? 1 : 0);
        PolledMeter.using(titusRuntime.getRegistry())
                .withId(resourceVersionId)
                .monitorValue(informer, i -> toLong(informer.lastSyncResourceVersion()));
        PolledMeter.using(titusRuntime.getRegistry())
                .withId(sizeId)
                .monitorValue(informer, i -> informer.getIndexer().list().size());
    }

    void close() {
        PolledMeter.remove(titusRuntime.getRegistry(), syncedId);
        PolledMeter.remove(titusRuntime.getRegistry(), watchingId);
        PolledMeter.remove(titusRuntime.getRegistry(), resourceVersionId);
        PolledMeter.remove(titusRuntime.getRegistry(), sizeId);
    }

    private long toLong(String lastSyncResourceVersion) {
        if (StringExt.isEmpty(lastSyncResourceVersion)) {
            return 0;
        }
        try {
            return Long.parseLong(lastSyncResourceVersion);
        } catch (Exception e) {
            return 0;
        }
    }
}
