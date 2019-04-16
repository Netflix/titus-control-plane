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
import java.util.Collections;

import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.RelocationLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;

/**
 * Set of predefined job profiles.
 */
public final class JobDescriptorCatalog {

    private static final DisruptionBudget EASY_TO_MIGRATE_DISRUPTION_BUDGET = DisruptionBudget.newBuilder()
            .withDisruptionBudgetPolicy(RelocationLimitDisruptionBudgetPolicy.newBuilder().withLimit(Integer.MAX_VALUE / 2).build())
            .withDisruptionBudgetRate(UnlimitedDisruptionBudgetRate.newBuilder().build())
            .withContainerHealthProviders(Collections.emptyList())
            .withTimeWindows(Collections.emptyList())
            .build();

    public enum ContainerResourceAllocation {
        Small, Medium, Large
    }

    private JobDescriptorCatalog() {
    }

    public static JobDescriptor<BatchJobExt> batchJob(ContainerResourceAllocation containerResourceAllocation, int instances, Duration duration) {
        int factor = 1;
        switch (containerResourceAllocation) {
            case Small:
                factor = 1;
                break;
            case Medium:
                factor = 4;
                break;
            case Large:
                factor = 10;
                break;
        }
        double cpu = factor;
        double memory = 512 * factor;
        double disk = 64 * factor;
        double network = 64 * factor;

        String script = String.format(
                "launched: delay=1s; startInitiated: delay=1s; started: delay=%ss; killInitiated: delay=1s",
                duration.getSeconds()
        );

        return JobDescriptorGenerator.oneTaskBatchJobDescriptor()
                .but(jd -> jd.getContainer()
                        .but(c -> {
                            return c.toBuilder()
                                    .withEnv(CollectionsExt.copyAndAdd(c.getEnv(), "TASK_LIFECYCLE_1", script))
                                    .withContainerResources(
                                            ContainerResources.newBuilder()
                                                    .withCpu(cpu)
                                                    .withMemoryMB((int) memory)
                                                    .withDiskMB((int) disk)
                                                    .withNetworkMbps((int) network)
                                                    .build()
                                    );
                        })
                )
                .but(jd -> jd.getExtensions().toBuilder()
                        .withSize(instances)
                        .withRuntimeLimitMs(Math.max(60_000, duration.toMillis() + 5_000))
                );
    }

    public static JobDescriptor<BatchJobExt> batchJobEasyToMigrate(ContainerResourceAllocation containerResourceAllocation, int instances, Duration duration) {
        return batchJob(containerResourceAllocation, instances, duration).toBuilder()
                .withDisruptionBudget(EASY_TO_MIGRATE_DISRUPTION_BUDGET)
                .build();
    }

    public static JobDescriptor<ServiceJobExt> serviceJob(ContainerResourceAllocation containerResourceAllocation, int min, int desired, int max) {
        int factor = 1;
        switch (containerResourceAllocation) {
            case Small:
                factor = 1;
                break;
            case Medium:
                factor = 2;
                break;
            case Large:
                factor = 8;
                break;
        }
        double cpu = factor;
        double memory = 512 * factor;
        double disk = 64 * factor;
        double network = 64 * factor;

        String script = "launched: delay=1s; startInitiated: delay=1s; started: delay=30d; killInitiated: delay=1s";

        return JobDescriptorGenerator.oneTaskServiceJobDescriptor()
                .but(jd -> jd.getContainer()
                        .but(c -> c.toBuilder()
                                .withEnv(CollectionsExt.copyAndAdd(c.getEnv(), "TASK_LIFECYCLE_1", script))
                                .withContainerResources(
                                        ContainerResources.newBuilder()
                                                .withCpu(cpu)
                                                .withMemoryMB((int) memory)
                                                .withDiskMB((int) disk)
                                                .withNetworkMbps((int) network)
                                                .build()
                                ))
                )
                .but(jd -> jd.getExtensions().toBuilder()
                        .withCapacity(Capacity.newBuilder().withMin(min).withDesired(desired).withMax(max).build())
                        .build()
                );
    }

    public static JobDescriptor<ServiceJobExt> longRunningServiceJob(String capacityGroup, int size) {
        String script = "launched: delay=1s; startInitiated: delay=30s; started: delay=30d; killInitiated: delay=120s";

        return JobDescriptorGenerator.oneTaskServiceJobDescriptor()
                .but(jd -> jd.toBuilder().withCapacityGroup(capacityGroup))
                .but(jd -> jd.getContainer()
                        .but(c -> c.toBuilder()
                                .withEnv(CollectionsExt.copyAndAdd(c.getEnv(), "TASK_LIFECYCLE_1", script))
                                .withContainerResources(
                                        ContainerResources.newBuilder()
                                                .withCpu(2.0)
                                                .withMemoryMB(4096)
                                                .withDiskMB(30720)
                                                .withNetworkMbps(128)
                                                .build()
                                ))
                )
                .but(jd -> jd.getExtensions().toBuilder()
                        .withCapacity(Capacity.newBuilder().withMin(0).withDesired(size).withMax(size * 2).build())
                        .build()
                );
    }

    public static JobDescriptor<ServiceJobExt> serviceJobEasyToMigrate(ContainerResourceAllocation containerResourceAllocation, int min, int desired, int max) {
        return serviceJob(containerResourceAllocation, min, desired, max).toBuilder()
                .withDisruptionBudget(EASY_TO_MIGRATE_DISRUPTION_BUDGET)
                .build();
    }
}
