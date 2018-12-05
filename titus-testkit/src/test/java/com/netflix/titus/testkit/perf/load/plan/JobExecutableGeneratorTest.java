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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.testkit.perf.load.plan.catalog.JobExecutionPlanCatalog;
import com.netflix.titus.testkit.perf.load.plan.catalog.JobDescriptorCatalog;
import com.netflix.titus.testkit.perf.load.plan.JobExecutableGenerator.Executable;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class JobExecutableGeneratorTest {

    private final ExtTestSubscriber<Executable> testSubscriber = new ExtTestSubscriber<>();

    @Test
    public void testSimpleScenario() {
        JobExecutableGenerator scenario = JobExecutableGenerator.newBuilder()
                .constantLoad(
                        JobDescriptorCatalog.batchJob(JobDescriptorCatalog.JobSize.Small, 1, 1, TimeUnit.HOURS),
                        JobExecutionPlanCatalog.uninterruptedJob(),
                        2
                ).build();

        scenario.executionPlans().subscribe(testSubscriber);

        // We expect only two jobs first
        List<JobExecutableGenerator.Executable> executables = testSubscriber.takeNext(2);
        Assertions.assertThat(testSubscriber.takeNext()).isNull();

        // Now return single job
        scenario.completed(executables.get(0));
        Assertions.assertThat(testSubscriber.takeNext()).isNotNull();
    }
}