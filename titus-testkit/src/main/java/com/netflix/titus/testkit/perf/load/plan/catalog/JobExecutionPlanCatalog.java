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

package com.netflix.titus.testkit.perf.load.plan.catalog;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;

public final class JobExecutionPlanCatalog {

    private JobExecutionPlanCatalog() {
    }

    public static ExecutionPlan uninterruptedJob() {
        return ExecutionPlan.jobExecutionPlan()
                .awaitCompletion()
                .build();
    }

    public static ExecutionPlan monitoredBatchJob() {
        return ExecutionPlan.jobExecutionPlan()
                .label("start")
                .delay(Duration.ofSeconds(30))
                .findOwnJob()
                .findOwnTasks()
                .loop("start")
                .build();
    }

    public static ExecutionPlan monitoredServiceJob(Duration duration) {
        return ExecutionPlan.jobExecutionPlan()
                .label("start")
                .totalRunningTime(duration)
                .findOwnJob()
                .findOwnTasks()
                .loop("start")
                .build();
    }

    public static ExecutionPlan batchWithKilledTasks(Duration killInterval) {
        return ExecutionPlan.jobExecutionPlan()
                .label("start")
                .killRandomTask()
                .delay(killInterval)
                .loop("start")
                .build();
    }

    public static ExecutionPlan serviceWithKilledTasks() {
        return ExecutionPlan.jobExecutionPlan()
                .label("start")
                .killRandomTask()
                .delay(30, TimeUnit.SECONDS)
                .loop("start")
                .build();
    }

    public static ExecutionPlan autoScalingService() {
        return ExecutionPlan.jobExecutionPlan()
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

    public static ExecutionPlan terminateAndShrinkAutoScalingService(Duration jobDuration, Duration stepInterval) {
        return ExecutionPlan.jobExecutionPlan()
                .totalRunningTime(jobDuration)
                .scaleUp(5)
                .label("start")
                .delay(stepInterval)
                .scaleUp(2)
                .label("shrinking")
                .delay(stepInterval)
                .terminateAndShrinkRandomTask()
                .loop("shrinking", 2)
                .delay(stepInterval)
                .scaleDown(1)
                .loop("start")
                .awaitCompletion()
                .build();
    }

    public static ExecutionPlan eviction() {
        return ExecutionPlan.jobExecutionPlan()
                .label("start")
                .evictRandomTask()
                .delay(30, TimeUnit.SECONDS)
                .loop("start")
                .build();
    }
}
