/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.Collections;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.UnlimitedDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.model.job.migration.MigrationPolicy;
import com.netflix.titus.api.jobmanager.model.job.migration.SelfManagedMigrationPolicy;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.gateway.MetricConstants;

@Singleton
public class DisruptionBudgetSanitizer {

    private static final String METRICS_ROOT = MetricConstants.METRIC_JOB_MANAGEMENT + "disruptionBudget.";

    private static final int MIN_BATCH_RELOCATION_TIME_MS = 60_000;

    @VisibleForTesting
    static final int DEFAULT_SERVICE_RELOCATION_TIME_MS = 60_000;

    /**
     * A strict alignment of a batch task runtime limit and a self migration deadline, may cause interruption of a batch task
     * which is almost finished. To prevent that we set migration limit to a slightly higher value.
     */
    @VisibleForTesting
    static final double BATCH_RUNTIME_LIMIT_FACTOR = 1.2;

    private final DisruptionBudgetSanitizerConfiguration configuration;
    private final Registry registry;

    private final Id nonCompliantId;

    @Inject
    public DisruptionBudgetSanitizer(DisruptionBudgetSanitizerConfiguration configuration, TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.registry = titusRuntime.getRegistry();
        this.nonCompliantId = registry.createId(METRICS_ROOT + "nonCompliant");
    }

    public JobDescriptor sanitize(JobDescriptor original) {
        if (!DisruptionBudgetFunctions.isLegacyJobDescriptor(original)) {
            return original;
        }

        record(original);

        return JobFunctions.isServiceJob(original)
                ? injectDefaultServiceDisruptionBudget(original)
                : injectDefaultBatchDisruptionBudget(original);
    }

    private JobDescriptor injectDefaultServiceDisruptionBudget(JobDescriptor<ServiceJobExt> original) {
        DisruptionBudget.Builder budgetBuilder = DisruptionBudget.newBuilder()
                .withDisruptionBudgetRate(UnlimitedDisruptionBudgetRate.newBuilder().build())
                .withContainerHealthProviders(Collections.emptyList())
                .withTimeWindows(Collections.emptyList());

        MigrationPolicy migrationPolicy = original.getExtensions().getMigrationPolicy();
        if (migrationPolicy instanceof SelfManagedMigrationPolicy) {
            budgetBuilder.withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder()
                    .withRelocationTimeMs(configuration.getServiceSelfManagedRelocationTimeMs())
                    .build()
            );
        } else {
            // If no policy defined, set short self managed to cause immediate fallback to the system default.
            budgetBuilder.withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder()
                    .withRelocationTimeMs(DEFAULT_SERVICE_RELOCATION_TIME_MS)
                    .build()
            );
        }

        return original.toBuilder().withDisruptionBudget(budgetBuilder.build()).build();
    }

    private JobDescriptor injectDefaultBatchDisruptionBudget(JobDescriptor<BatchJobExt> original) {
        long runtimeLimitMs = Math.max(
                MIN_BATCH_RELOCATION_TIME_MS,
                (long) (original.getExtensions().getRuntimeLimitMs() * BATCH_RUNTIME_LIMIT_FACTOR)
        );

        DisruptionBudget.Builder budgetBuilder = DisruptionBudget.newBuilder()
                .withDisruptionBudgetPolicy(SelfManagedDisruptionBudgetPolicy.newBuilder()
                        .withRelocationTimeMs(runtimeLimitMs)
                        .build()
                )
                .withDisruptionBudgetRate(UnlimitedDisruptionBudgetRate.newBuilder().build())
                .withContainerHealthProviders(Collections.emptyList())
                .withTimeWindows(Collections.emptyList());

        return original.toBuilder().withDisruptionBudget(budgetBuilder.build()).build();
    }

    private void record(JobDescriptor original) {
        registry.counter(nonCompliantId.withTags(
                "capacityGroup", original.getCapacityGroup(),
                "application", original.getApplicationName(),
                "jobType", JobFunctions.isServiceJob(original) ? "service" : "batch"
        )).increment();
    }
}
