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

import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator;

public final class JobExecutableGeneratorCatalog {

    private JobExecutableGeneratorCatalog() {
    }

    /**
     * A mix of service/batch jobs with different sizes and task counts.
     */
    public static JobExecutableGenerator mixedLoad(double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.batchJob(JobDescriptorCatalog.JobSize.Small, 1, 5, TimeUnit.MINUTES),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor * 100
                )
                .constantLoad(
                        JobDescriptorCatalog.batchJob(JobDescriptorCatalog.JobSize.Large, 5, 30, TimeUnit.MINUTES),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor * 20
                )
                .constantLoad(
                        JobDescriptorCatalog.serviceJob(JobDescriptorCatalog.JobSize.Medium, 0, 25, 50),
                        JobExecutionPlanCatalog.autoScalingService(),
                        (int) sizeFactor * 50
                )
                .build();
    }

    public static JobExecutableGenerator batchJobs(int jobSize, double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.batchJob(JobDescriptorCatalog.JobSize.Small, jobSize, 60, TimeUnit.SECONDS),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor
                )
                .build();
    }

    public static JobExecutableGenerator evictions(int jobSize, double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.serviceJob(JobDescriptorCatalog.JobSize.Small, 0, jobSize, jobSize),
                        JobExecutionPlanCatalog.eviction(),
                        (int) sizeFactor
                )
                .build();
    }

    public static JobExecutableGenerator oneAutoScalingService(double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.serviceJob(JobDescriptorCatalog.JobSize.Small, 0, 1, 100),
                        JobExecutionPlanCatalog.autoScalingService(),
                        (int) sizeFactor
                )
                .build();
    }

    public static JobExecutableGenerator oneScalingServiceWihTerminateAndShrink(double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.serviceJob(JobDescriptorCatalog.JobSize.Small, 0, 1, 100),
                        JobExecutionPlanCatalog.terminateAndShrinkAutoScalingService(),
                        (int) sizeFactor
                )
                .build();
    }
}
