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

package com.netflix.titus.master.jobmanager.service.integration;

import java.util.Collections;

import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeDisruptionBudget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.officeHourTimeWindow;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.percentageOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.selfManagedPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class JobDisruptionBudgetTest {

    private static final DisruptionBudget SELF_MANAGED = budget(
            selfManagedPolicy(10_000), unlimitedRate(), Collections.singletonList(officeHourTimeWindow())
    );

    private static final DisruptionBudget PERCENTAGE = budget(
            percentageOfHealthyPolicy(80), unlimitedRate(), Collections.singletonList(officeHourTimeWindow())
    );

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @Test
    public void testDisruptionBudgetUpdate() {
        JobDescriptor<ServiceJobExt> jobWithSelfManaged = changeDisruptionBudget(oneTaskServiceJobDescriptor(), SELF_MANAGED);

        jobsScenarioBuilder.scheduleJob(jobWithSelfManaged, jobScenario -> jobScenario
                .expectJobEvent()
                .changeDisruptionBudget(PERCENTAGE)
                .expectJobUpdatedInStore(job -> assertThat(job.getJobDescriptor().getDisruptionBudget()).isEqualTo(PERCENTAGE))
                .expectJobEvent(job -> assertThat(job.getJobDescriptor().getDisruptionBudget()).isEqualTo(PERCENTAGE))
        );
    }
}
