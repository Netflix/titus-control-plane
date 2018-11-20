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

package com.netflix.titus.testkit.perf.load.catalog;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;

public final class ExecutionPlanCatalog {

    private ExecutionPlanCatalog() {
    }

    public static ExecutionPlan uninterruptedJob() {
        return ExecutionPlan.newBuilder()
                .awaitCompletion()
                .build();
    }

    public static ExecutionPlan serviceWithKilledTasks() {
        return ExecutionPlan.newBuilder()
                .label("start")
                .killRandomTask()
                .delay(30, TimeUnit.SECONDS)
                .loop("start")
                .build();
    }

    public static ExecutionPlan autoScalingService() {
        return ExecutionPlan.newBuilder()
                .label("start")
                .scaleUp(50)
                .scaleUp(40)
                .delay(30, TimeUnit.SECONDS)
                .scaleDown(70)
                .scaleDown(20)
                .loop("start")
                .awaitCompletion()
                .build();
    }

    public static ExecutionPlan terminateAndShrinkAutoScalingService() {
        return ExecutionPlan.newBuilder()
                .scaleUp(5)
                .label("start")
                .scaleUp(10)
                .scaleUp(10)
                .scaleUp(10)
                .delay(30, TimeUnit.SECONDS)
                .label("shrinking")
                .terminateAndShrinkRandomTask()
                .loop("shrinking", 19)
                .scaleDown(10)
                .loop("start")
                .awaitCompletion()
                .build();
    }

    public static ExecutionPlan eviction() {
        return ExecutionPlan.newBuilder()
                .label("start")
                .evictRandomTask()
                .delay(30, TimeUnit.SECONDS)
                .loop("start")
                .build();
    }
}
