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

import com.netflix.titus.testkit.perf.load.plan.ExecutionScenario;

public final class ExecutionScenarioCatalog {

    private ExecutionScenarioCatalog() {
    }

    /**
     * A mix of service/batch jobs with different sizes and task counts.
     */
    public static ExecutionScenario mixedLoad(double sizeFactor) {
        return ExecutionScenario.newBuilder()
                .constantLoad(
                        JobCatalog.batchJob(JobCatalog.JobSize.Small, 1, 5, TimeUnit.MINUTES),
                        ExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor * 100
                )
                .constantLoad(
                        JobCatalog.batchJob(JobCatalog.JobSize.Large, 5, 30, TimeUnit.MINUTES),
                        ExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor * 20
                )
                .constantLoad(
                        JobCatalog.serviceJob(JobCatalog.JobSize.Medium, 0, 25, 50),
                        ExecutionPlanCatalog.autoScalingService(),
                        (int) sizeFactor * 50
                )
                .build();
    }

    public static ExecutionScenario batchJob(int jobSize, double sizeFactor) {
        return ExecutionScenario.newBuilder()
                .constantLoad(
                        JobCatalog.batchJob(JobCatalog.JobSize.Small, jobSize, 60, TimeUnit.SECONDS),
                        ExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor
                )
                .build();
    }

    public static ExecutionScenario oneAutoScalingService(double sizeFactor) {
        return ExecutionScenario.newBuilder()
                .constantLoad(
                        JobCatalog.serviceJob(JobCatalog.JobSize.Small, 0, 1, 100),
                        ExecutionPlanCatalog.autoScalingService(),
                        (int) sizeFactor
                )
                .build();
    }

    public static ExecutionScenario oneScalingServiceWihTerminateAndShrink(double sizeFactor) {
        return ExecutionScenario.newBuilder()
                .constantLoad(
                        JobCatalog.serviceJob(JobCatalog.JobSize.Small, 0, 1, 100),
                        ExecutionPlanCatalog.terminateAndShrinkAutoScalingService(),
                        (int) sizeFactor
                )
                .build();
    }
}
