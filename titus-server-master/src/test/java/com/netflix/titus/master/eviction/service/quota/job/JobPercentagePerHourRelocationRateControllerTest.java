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

package com.netflix.titus.master.eviction.service.quota.job;

import java.time.Duration;
import java.util.Collections;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.TestClock;
import org.junit.Test;

import static com.netflix.titus.master.eviction.service.quota.job.JobPercentagePerHourRelocationRateController.newJobPercentagePerHourRelocationRateController;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.exceptRate;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.hourlyRatePercentage;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJob;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static org.assertj.core.api.Assertions.assertThat;

public class JobPercentagePerHourRelocationRateControllerTest {

    private static final Job<BatchJobExt> REFERENCE_JOB = newBatchJob(
            10,
            budget(percentageOfHealthyPolicy(100), hourlyRatePercentage(100), Collections.emptyList())
    );
    private static final Reference JOB_REFERENCE = Reference.job(REFERENCE_JOB.getId());

    private static final Duration TEN_MINUTES = Duration.ofMinutes(10);

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestClock clock = (TestClock) titusRuntime.getClock();

    @Test
    public void testQuota() {
        JobPercentagePerHourRelocationRateController quotaController = newJobPercentagePerHourRelocationRateController(
                exceptRate(REFERENCE_JOB, hourlyRatePercentage(50)),
                SelfJobDisruptionBudgetResolver.getInstance(),
                titusRuntime
        );

        assertThat(quotaController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(5);

        // Consume everything
        consumeAtInterval(quotaController, 5, TEN_MINUTES);
        assertThat(quotaController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(0);

        // Now shift time and consume again
        clock.advanceTime(TEN_MINUTES);
        clock.advanceTime(TEN_MINUTES);
        assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();

        // Now move long into the future
        clock.advanceTime(Duration.ofHours(2));
        assertThat(quotaController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(5);
    }

    @Test
    public void testJobUpdate() {
        JobPercentagePerHourRelocationRateController firstController = newJobPercentagePerHourRelocationRateController(
                exceptRate(REFERENCE_JOB, hourlyRatePercentage(50)),
                SelfJobDisruptionBudgetResolver.getInstance(),
                titusRuntime
        );

        // Take all
        consumeAtInterval(firstController, 5, TEN_MINUTES);
        assertThat(firstController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(0);

        // Now increase the allowance
        JobPercentagePerHourRelocationRateController updatedController = firstController.update(exceptRate(REFERENCE_JOB, hourlyRatePercentage(80)));
        assertThat(updatedController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(3);
    }

    private void consumeAtInterval(JobPercentagePerHourRelocationRateController quotaController, int count, Duration interval) {
        for (int i = 0; i < count; i++) {
            clock.advanceTime(interval);
            assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();
        }
    }
}