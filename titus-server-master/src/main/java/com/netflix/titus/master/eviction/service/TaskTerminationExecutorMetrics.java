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

package com.netflix.titus.master.eviction.service;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.MetricConstants;

class TaskTerminationExecutorMetrics {

    private static final String ROOT_NAME = MetricConstants.METRIC_SCHEDULING_EVICTION + "taskTerminationExecutor.";
    private static final String TERMINATE_TASK = ROOT_NAME + "terminateTask";

    private final Registry registry;

    private final Counter terminatedCounter;
    private final Id unexpectedErrorId;

    TaskTerminationExecutorMetrics(TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();

        this.terminatedCounter = registry.counter(TERMINATE_TASK, "status", "success");
        this.unexpectedErrorId = registry.createId(TERMINATE_TASK, "status", "error");
    }

    void terminated() {
        terminatedCounter.increment();
    }

    void error(Throwable error) {
        String errorCode = error instanceof EvictionException
                ? "" + ((EvictionException) error).getErrorCode()
                : "unknown";
        registry.counter(unexpectedErrorId.withTags("exception", error.getClass().getSimpleName(), "errorCode", errorCode));
    }
}
