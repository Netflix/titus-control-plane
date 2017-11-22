/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service.integration;

import io.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import org.junit.Test;

import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

public class BatchJobSchedulingTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @Test
    public void testStartFinishOk() throws Exception {
        jobsScenarioBuilder.scheduleBatchJob(oneTaskBatchJobDescriptor(), jobScenario -> jobScenario
                .template(ScenarioTemplates.startSingleTaskJob())
                .template(ScenarioTemplates.finishSingleTaskJob())
        );
    }

    @Test
    public void testStartFinishWitNonZeroErrorCode() throws Exception {
    }

    @Test
    public void testKillInAcceptedState() throws Exception {
    }

    @Test
    public void testKillInStartInitiatedState() throws Exception {
    }

    @Test
    public void testKillInStartedState() throws Exception {
    }

    @Test
    public void testKillInKillInitiatedState() throws Exception {
    }

    @Test
    public void testLaunchingTimeout() throws Exception {
    }

    @Test
    public void testStartInitiatedTimeout() throws Exception {
    }

    @Test
    public void testKillInitiatedTimeout() throws Exception {
    }

    @Test
    public void testImmediateRetry() throws Exception {
    }

    @Test
    public void testDelayedRetry() throws Exception {
    }

    @Test
    public void testExponentialBackoffRetry() throws Exception {
    }

    @Test
    public void testLargeJobRateLimiting() throws Exception {
    }

    @Test
    public void testLargeJobWithFailingTasksRateLimiting() throws Exception {
    }
}
