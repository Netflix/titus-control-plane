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

package com.netflix.titus.master.eviction.service.quota.system;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Month;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.Day;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.testkit.rx.ReactorTestExt;
import com.netflix.titus.testkit.rx.internal.DirectedProcessor;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.EmitterProcessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SystemQuotaControllerTest {

    private static TestClock clock = Clocks.testWorldClock(2000, Month.JANUARY, 1).jumpForwardTo(DayOfWeek.MONDAY);

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);

    private final DirectedProcessor<SystemDisruptionBudget> budgetEmitter = ReactorTestExt.newDirectedProcessor(() -> EmitterProcessor.create(1));

    private final SystemDisruptionBudgetResolver budgetResolver = mock(SystemDisruptionBudgetResolver.class);

    private SystemQuotaController quotaController;

    @Before
    public void setUp() throws Exception {
        when(budgetResolver.resolve()).thenReturn(budgetEmitter);
    }

    @Test
    public void testQuotaConsumption() {
        budgetEmitter.onNext(SystemDisruptionBudget.newBasicSystemDisruptionBudget(1, 1));
        quotaController = newSystemQuotaController();

        assertThat(quotaController.getQuota(Reference.system()).getQuota()).isEqualTo(1);
        assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();
    }

    @Test
    public void testOutsideTimeWindow() {
        budgetEmitter.onNext(SystemDisruptionBudget.newBasicSystemDisruptionBudget(
                1,
                1,
                TimeWindow.newBuilder()
                        .withDays(Day.weekdays())
                        .withwithHourlyTimeWindows(8, 16)
                        .build()
        ));

        // Outside time window
        quotaController = newSystemQuotaController();
        assertThat(quotaController.getQuota(Reference.system()).getQuota()).isEqualTo(0);
        assertThat(quotaController.consume("someTaskId").isApproved()).isFalse();

        // In time window
        clock.resetTime(10, 0, 0);
        assertThat(quotaController.getQuota(Reference.system()).getQuota()).isEqualTo(1);
        assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();
    }

    @Test
    public void testBudgetReconfiguration() {
        // Large quota
        budgetEmitter.onNext(SystemDisruptionBudget.newBasicSystemDisruptionBudget(5, 5));
        quotaController = newSystemQuotaController();
        assertThat(quotaController.getQuota(Reference.system()).getQuota()).isEqualTo(5);

        // Small quota
        budgetEmitter.onNext(SystemDisruptionBudget.newBasicSystemDisruptionBudget(1, 1));
        assertThat(quotaController.getQuota(Reference.system()).getQuota()).isEqualTo(1);
        assertThat(quotaController.consume("someTaskId").isApproved()).isTrue();
        assertThat(quotaController.consume("someTaskId").isApproved()).isFalse();
    }

    @Test
    public void testRetryOnFailure() {
        budgetEmitter.onNext(SystemDisruptionBudget.newBasicSystemDisruptionBudget(1, 1));
        quotaController = newSystemQuotaController();

        // Emit error
        budgetEmitter.onError(new RuntimeException("Simulated error"));
        budgetEmitter.onNext(SystemDisruptionBudget.newBasicSystemDisruptionBudget(5, 5));
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(() -> quotaController.getQuota(Reference.system()).getQuota() == 5);
    }

    private SystemQuotaController newSystemQuotaController() {
        return new SystemQuotaController(budgetResolver, Duration.ofMillis(1), titusRuntime);
    }
}