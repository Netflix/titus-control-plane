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
import java.util.Arrays;
import java.util.List;

import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator;
import com.netflix.titus.testkit.perf.load.plan.catalog.JobDescriptorCatalog.ContainerResourceAllocation;
import reactor.core.publisher.Flux;

public final class JobExecutableGeneratorCatalog {

    private static final JobExecutableGenerator EMPTY = new JobExecutableGenerator() {
        @Override
        public Flux<Executable> executionPlans() {
            return Flux.never();
        }

        @Override
        public void completed(Executable executable) {
        }
    };

    static final List<Pair<Integer, Integer>> LONG_RUNNING_SIZES_AND_COUNTS = Arrays.asList(
            Pair.of(100, 5),
            Pair.of(200, 3),
            Pair.of(500, 3),
            Pair.of(1000, 4),
            Pair.of(5000, 1),
            Pair.of(10000, 1)
    );

    private JobExecutableGeneratorCatalog() {
    }

    /**
     * Job execution scenario that does not create any job
     */
    public static JobExecutableGenerator empty() {
        return EMPTY;
    }

    /**
     * A mix of service/batch jobs with different sizes and task counts.
     */
    public static JobExecutableGenerator mixedLoad(double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.batchJob(ContainerResourceAllocation.Small, 1, Duration.ofMinutes(5)),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor * 100
                )
                .constantLoad(
                        JobDescriptorCatalog.batchJob(ContainerResourceAllocation.Large, 5, Duration.ofMinutes(30)),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        (int) sizeFactor * 20
                )
                .constantLoad(
                        JobDescriptorCatalog.serviceJob(ContainerResourceAllocation.Medium, 0, 25, 200),
                        JobExecutionPlanCatalog.autoScalingService(),
                        (int) sizeFactor * 50
                )
                .build();
    }

    /**
     * A mix of service/batch jobs for system performance testing with the wide functional area coverage.
     */
    public static JobExecutableGenerator perfLoad(double sizeFactor) {
        return JobExecutableGenerator.newBuilder()
                // Batch
                .constantLoad(
                        JobDescriptorCatalog.batchJobEasyToMigrate(ContainerResourceAllocation.Small, 1, Duration.ofMinutes(5)),
                        JobExecutionPlanCatalog.batchWithKilledTasks(),
                        (int) sizeFactor * 20
                )
                .constantLoad(
                        JobDescriptorCatalog.batchJobEasyToMigrate(ContainerResourceAllocation.Medium, 10, Duration.ofMinutes(10)),
                        JobExecutionPlanCatalog.monitoredBatchJob(),
                        (int) sizeFactor * 10
                )
                .constantLoad(
                        JobDescriptorCatalog.batchJobEasyToMigrate(ContainerResourceAllocation.Large, 5, Duration.ofMinutes(30)),
                        JobExecutionPlanCatalog.batchWithKilledTasks(),
                        (int) sizeFactor * 3
                )
                // Service
                .constantLoad(
                        JobDescriptorCatalog.serviceJobEasyToMigrate(ContainerResourceAllocation.Small, 0, 5, 5),
                        JobExecutionPlanCatalog.monitoredServiceJob(Duration.ofMinutes(10)),
                        (int) sizeFactor * 20
                )
                .constantLoad(
                        JobDescriptorCatalog.serviceJobEasyToMigrate(ContainerResourceAllocation.Medium, 0, 10, 100),
                        JobExecutionPlanCatalog.terminateAndShrinkAutoScalingService(Duration.ofMinutes(10)),
                        (int) sizeFactor * 10
                )
                .constantLoad(
                        JobDescriptorCatalog.serviceJobEasyToMigrate(ContainerResourceAllocation.Large, 5, 10, 100),
                        JobExecutionPlanCatalog.terminateAndShrinkAutoScalingService(Duration.ofMinutes(30)),
                        (int) sizeFactor * 10
                )
                .constantLoad(
                        JobDescriptorCatalog.serviceJobEasyToMigrate(ContainerResourceAllocation.Large, 5, 500, 1000),
                        JobExecutionPlanCatalog.monitoredServiceJob(Duration.ofMinutes(60)),
                        (int) sizeFactor * 1
                )
                .build();
    }

    public static JobExecutableGenerator longRunningServicesLoad(String capacityGroup) {
        JobExecutableGenerator.ExecutionScenarioBuilder builder = JobExecutableGenerator.newBuilder();
        for (int i = 0; i < LONG_RUNNING_SIZES_AND_COUNTS.size(); i++) {
            int size = LONG_RUNNING_SIZES_AND_COUNTS.get(i).getLeft();
            int count = LONG_RUNNING_SIZES_AND_COUNTS.get(i).getRight();
            Duration totalDuration;
            if (size > 1000) {
                totalDuration = Duration.ofMinutes(30);
            } else {
                totalDuration = i % 2 == 0 ? Duration.ofMinutes(10) : Duration.ofMinutes(20);
            }
            builder.constantLoad(
                    JobDescriptorCatalog.longRunningServiceJob(capacityGroup, size),
                    JobExecutionPlanCatalog.terminateAndShrinkAutoScalingService(totalDuration),
                    count
            );
        }
        return builder.build();
    }

    public static JobExecutableGenerator batchJobs(int jobSize, int numberOfJobs) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.batchJob(ContainerResourceAllocation.Small, jobSize, Duration.ofSeconds(60)),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        numberOfJobs
                )
                .build();
    }

    public static JobExecutableGenerator evictions(int jobSize, int numberOfJobs) {
        return JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.serviceJob(ContainerResourceAllocation.Small, 0, jobSize, jobSize),
                        JobExecutionPlanCatalog.eviction(),
                        numberOfJobs
                )
                .build();
    }
}
