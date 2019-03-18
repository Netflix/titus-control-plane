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

import static com.netflix.titus.master.eviction.service.quota.job.JobPercentagePerIntervalRateController.newJobPercentagePerIntervalRateController;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.exceptRate;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJob;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.ratePercentagePerInterval;
import static org.assertj.core.api.Assertions.assertThat;

public class JobPercentagePerIntervalRateControllerTest {

    private static final Job<BatchJobExt> REFERENCE_JOB = newBatchJob(
            10,
            budget(percentageOfHealthyPolicy(100), ratePercentagePerInterval(600_000, 100), Collections.emptyList())
    );
    private static final Reference JOB_REFERENCE = Reference.job(REFERENCE_JOB.getId());

    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final TestClock clock = (TestClock) titusRuntime.getClock();

    @Test
    public void testQuota() {
        JobPercentagePerIntervalRateController quotaController = newJobPercentagePerIntervalRateController(
                exceptRate(REFERENCE_JOB, ratePercentagePerInterval(600_000, 50)),
                SelfJobDisruptionBudgetResolver.getInstance(),
                titusRuntime
        );

        assertThat(quotaController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(5);

        // Consume everything
        consumeAtInterval(quotaController, 5, ONE_MINUTE);
        assertThat(quotaController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(0);

        // Now shift time and consume again
        clock.advanceTime(ONE_MINUTE.multipliedBy(5));
        clock.advanceTime(ONE_MINUTE);
        assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();

        // Now move long into the future
        clock.advanceTime(Duration.ofHours(2));
        assertThat(quotaController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(5);
    }

    @Test
    public void testJobUpdate() {
        JobPercentagePerIntervalRateController firstController = newJobPercentagePerIntervalRateController(
                exceptRate(REFERENCE_JOB, ratePercentagePerInterval(600_000, 50)),
                SelfJobDisruptionBudgetResolver.getInstance(),
                titusRuntime
        );

        // Take all
        consumeAtInterval(firstController, 5, ONE_MINUTE);
        assertThat(firstController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(0);

        // Now increase the allowance
        JobPercentagePerIntervalRateController updatedController = firstController.update(exceptRate(REFERENCE_JOB, ratePercentagePerInterval(150_000, 80)));
        assertThat(updatedController.getQuota(JOB_REFERENCE).getQuota()).isEqualTo(3);
    }

    private void consumeAtInterval(JobPercentagePerIntervalRateController quotaController, int count, Duration interval) {
        for (int i = 0; i < count; i++) {
            clock.advanceTime(interval);
            assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();
        }
    }
}