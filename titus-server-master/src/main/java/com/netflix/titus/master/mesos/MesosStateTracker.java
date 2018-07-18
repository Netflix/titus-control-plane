/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.mesos;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.config.MasterConfiguration;
import org.apache.mesos.Protos;

/**
 * Tracks all running containers, by observing task status update requests. We depend here on the
 * reconciliation task periodically requesting full Mesos state. To avoid stale data in local cache,
 * known tasks are kept for two reconciler cycles (as we expect to refresh their state at each cycle), and
 * the unknown tasks are kept for four cycles, as every other cycle only requests status of the all known tasks.
 */
class MesosStateTracker {

    private static final String TASKS_METRIC_ID = MetricConstants.METRIC_MESOS + "tasks";

    private final Cache<String, Protos.TaskStatus> knownTasks;
    private final Cache<String, Protos.TaskStatus> unknownTasks;

    private final Registry registry;

    MesosStateTracker(MasterConfiguration configuration, TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();

        this.knownTasks = CacheBuilder.newBuilder()
                .expireAfterWrite(configuration.getMesosTaskReconciliationIntervalSecs() * 2, TimeUnit.SECONDS)
                .build();
        this.unknownTasks = CacheBuilder.newBuilder()
                .expireAfterWrite(configuration.getMesosTaskReconciliationIntervalSecs() * 4, TimeUnit.SECONDS)
                .build();

        PolledMeter.using(registry).withId(registry.createId(TASKS_METRIC_ID, "known", "true")).monitorValue(this, self -> self.knownTasks.size());
        PolledMeter.using(registry).withId(registry.createId(TASKS_METRIC_ID, "known", "false")).monitorValue(this, self -> self.unknownTasks.size());
    }

    void knownTaskStatusUpdate(Protos.TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();
        knownTasks.put(taskId, taskStatus);
        unknownTasks.invalidate(taskId);

    }

    void unknownTaskStatusUpdate(Protos.TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();
        knownTasks.invalidate(taskId);
        unknownTasks.put(taskId, taskStatus);
    }
}
