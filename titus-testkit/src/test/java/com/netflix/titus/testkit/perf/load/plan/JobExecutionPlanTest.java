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

package com.netflix.titus.testkit.perf.load.plan;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobExecutionPlanTest {

    @Test
    public void testSimplePlan() {
        ExecutionPlan plan = ExecutionPlan.jobExecutionPlan()
                .label("start")
                .delay(5, TimeUnit.SECONDS)
                .scaleUp(5)
                .delay(5, TimeUnit.SECONDS)
                .scaleDown(5)
                .loop("start", 1)
                .loop("start", 1)
                .build();

        Iterator<ExecutionStep> planIterator = plan.newInstance();

        for (int i = 0; i < 4; i++) {
            assertThat(planIterator.next()).isEqualTo(JobExecutionStep.delay(5, TimeUnit.SECONDS));
            assertThat(planIterator.next()).isEqualTo(JobExecutionStep.scaleUp(5));
            assertThat(planIterator.next()).isEqualTo(JobExecutionStep.delay(5, TimeUnit.SECONDS));
            assertThat(planIterator.next()).isEqualTo(JobExecutionStep.scaleDown(5));
        }
        assertThat(planIterator.next()).isEqualTo(JobExecutionStep.terminate());
    }
}