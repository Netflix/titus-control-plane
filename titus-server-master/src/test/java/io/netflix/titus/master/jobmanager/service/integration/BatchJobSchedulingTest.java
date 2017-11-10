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

import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder;
import io.netflix.titus.master.mesos.TitusExecutorDetails;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.junit.Test;

import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class BatchJobSchedulingTest {

    /**
     * TODO All changes are in the form: [ChangeRequest, Updates..., Changed]. Scenario validator should explicitly model that.
     */
    @Test
    public void testStartFinishOk() throws Exception {
        String detailsWithIp = ObjectMappers.defaultMapper().writeValueAsString(new TitusExecutorDetails(
                CollectionsExt.asMap("nfvpc", "1.2.3.4"),
                new TitusExecutorDetails.NetworkConfiguration(
                        true, "1.2.3.4", "1.1.1.1", "eni-12345", "eni-resource-1"
                )));
        new JobScenarioBuilder<>(oneTaskBatchJobDescriptor())
                .activate()
                .submit()
                .expectStoreJobUpdate().trigger()
                .expectJobUpdateEvent()
                .expectStoreTaskAdded()
                .expectTaskCreatedEvent().advance()
                .expectScheduleRequest()
                .ignoreAvailableEvents()
                .triggerMesosEvent(0, TaskState.Launched)
                .expectTaskUpdateEvent(0, "Starting new task")
                .advance()
                .expectStoreTaskAdded()
                .ignoreAvailableEvents()
                .triggerMesosEvent(0, TaskState.StartInitiated)
                .advance()
                .expectStoreTaskAdded()
                .ignoreAvailableEvents()
                .triggerMesosEvent(0, TaskState.Started, "update", detailsWithIp)
                .advance()
                .expectStoreTaskAdded(task -> assertThat(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP))
                        .isEqualTo("1.2.3.4"))
                .ignoreAvailableEvents()
                .triggerMesosEvent(0, TaskState.Finished, TaskStatus.REASON_NORMAL)
                .advance()
                .expectTaskUpdateEvent(0, "Persisting task to the store")
                .expectTaskUpdateEvent(0, "Mesos -> Finished")
                .expectTaskUpdateEvent(0, "Updating task")
                .expectTaskUpdateEvent(0, "Updating task")
                .expectStoreTaskArchived()
                .expectStoreTaskRemoved()
                .expectStoreJobRemoved()
                .advance()
                .expectTaskUpdateEvent(0, "Persisting task to the store")
                .expectTaskUpdateEvent(0, "Persisting task to the store")
                .expectTaskUpdateEvent(0, "Persisting task to the store")
                .expectJobUpdateEvent()
                .expectJobUpdateEvent();
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
