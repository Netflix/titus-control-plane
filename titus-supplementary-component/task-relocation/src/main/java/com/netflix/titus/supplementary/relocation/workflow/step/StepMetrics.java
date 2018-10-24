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

package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.supplementary.relocation.RelocationMetrics;

class StepMetrics {

    private final Counter successCounter;
    private final Counter failureCounter;

    private final Timer successExecutionTime;
    private final Timer failureExecutionTime;

    StepMetrics(String stepName, TitusRuntime titusRuntime) {
        Registry registry = titusRuntime.getRegistry();

        Id baseCounterId = registry.createId(RelocationMetrics.METRIC_ROOT + "steps", "step", stepName);
        this.successCounter = registry.counter(baseCounterId.withTag("status", "success"));
        this.failureCounter = registry.counter(baseCounterId.withTag("status", "failure"));

        Id baseTimerId = registry.createId(RelocationMetrics.METRIC_ROOT + "steps", "stepExecutionTime", stepName);
        this.successExecutionTime = registry.timer(baseTimerId.withTag("status", "success"));
        this.failureExecutionTime = registry.timer(baseTimerId.withTag("status", "failure"));
    }

    void onSuccess(int resultSetSize, long elapsed) {
        successCounter.increment(resultSetSize);
        successExecutionTime.record(elapsed, TimeUnit.MILLISECONDS);
    }

    void onError(long elapsed) {
        failureCounter.increment();
        failureExecutionTime.record(elapsed, TimeUnit.MILLISECONDS);
    }
}
