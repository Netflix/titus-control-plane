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

package com.netflix.titus.testkit.perf.load.plan;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

public abstract class ExecutionPlanBuilder<BUILDER extends ExecutionPlanBuilder<BUILDER>> {

    protected Duration totalRunningTime = Duration.ofDays(30);
    protected final List<ExecutionStep> steps = new ArrayList<>();
    protected final Map<String, Integer> labelPos = new HashMap<>();

    public BUILDER totalRunningTime(Duration totalRunningTime) {
        this.totalRunningTime = totalRunningTime;
        return self();
    }

    public BUILDER label(String name) {
        labelPos.put(name, steps.size());
        return self();
    }

    public BUILDER loop(String label) {
        return loop(label, -1);
    }

    public BUILDER loop(String label, int times) {
        Preconditions.checkArgument(labelPos.containsKey(label), "Execution plan has no label " + label);
        steps.add(JobExecutionStep.loop(labelPos.get(label), times));
        return self();
    }

    public BUILDER delay(long duration, TimeUnit timeUnit) {
        steps.add(ExecutionStep.delay(duration, timeUnit));
        return self();
    }

    public BUILDER randomDelay(Duration lowerBound, Duration upperBound) {
        steps.add(ExecutionStep.randomDelayStep(lowerBound, upperBound));
        return self();
    }

    public abstract ExecutionPlan build();

    public BUILDER delay(Duration duration) {
        steps.add(JobExecutionStep.delay(duration.toMillis(), TimeUnit.MILLISECONDS));
        return self();
    }

    private BUILDER self() {
        return (BUILDER) this;
    }
}
