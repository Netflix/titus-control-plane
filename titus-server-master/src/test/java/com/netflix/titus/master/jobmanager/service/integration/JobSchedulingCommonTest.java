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

import java.util.UUID;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.jobmanager.service.integration.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.jobmanager.service.integration.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobSchedulingCommonTest {

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder();

    @Test
    public void testBatchJobWithTaskInAcceptedStateNotScheduledYet() {
        testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptorGenerator.oneTaskBatchJobDescriptor());
    }

    @Test
    public void testServiceJobWithTaskInAcceptedStateNotScheduledYet() {
        testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptorGenerator.oneTaskServiceJobDescriptor());
    }

    @Test
    public void testBatchJobWithFederatedJobId() {
        String federatedJobId = UUID.randomUUID().toString();
        testJobWithTaskInAcceptedStateNotScheduledYetWithFederatedJobId(JobDescriptorGenerator.oneTaskBatchJobDescriptorWithAttributes(
                CollectionsExt.<String, String>newHashMap().entry(JobAttributes.JOB_ATTRIBUTES_FEDERATED_JOB_ID, federatedJobId).build()),
                federatedJobId);
    }

    @Test
    public void testServiceJobWithFederatedJobId() {
        String federatedJobId = UUID.randomUUID().toString();
        testJobWithTaskInAcceptedStateNotScheduledYetWithFederatedJobId(JobDescriptorGenerator.oneTaskServiceJobDescriptorWithAttributes(
                CollectionsExt.<String, String>newHashMap().entry(JobAttributes.JOB_ATTRIBUTES_FEDERATED_JOB_ID, federatedJobId).build()),
                federatedJobId);
    }

    private void testJobWithTaskInAcceptedStateNotScheduledYetWithFederatedJobId(JobDescriptor<?> oneTaskJobDescriptor, String federatedJobId) {
        jobsScenarioBuilder.scheduleJob(oneTaskJobDescriptor, jobScenario ->
                jobScenario.expectJobEvent(job -> {
                    assertThat(job.getId()).isEqualTo(federatedJobId);
                    assertThat(job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_ORIGINAL_FEDERATED_JOB_ID)).isEqualTo(federatedJobId);
                    assertThat(job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_FEDERATED_JOB_ID)).isNull();
                }));
    }

    /**
     * This test covers the case where a task is created in store, but not added to the compute provider yet, and
     * the job while in this state is terminated.
     */
    private void testJobWithTaskInAcceptedStateNotScheduledYet(JobDescriptor<?> oneTaskJobDescriptor) {
        jobsScenarioBuilder.scheduleJob(oneTaskJobDescriptor, jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskAddedToStore(0, 0, task -> assertThat(task.getStatus().getState()).isEqualTo(TaskState.Accepted))
                .template(ScenarioTemplates.killJob())
                .expectTaskStateChangeEvent(0, 0, TaskState.Accepted)
                .template(ScenarioTemplates.handleTaskFinishedTransitionInSingleTaskJob(0, 0, TaskStatus.REASON_TASK_KILLED))
        );
    }
}
