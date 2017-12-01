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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;

public class ScenarioTemplates {

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> startSingleTaskJob() {
        return jobScenario -> jobScenario
                .expectJobEvent()
                .expectTaskAddedToStore(task -> task.getStatus().getState() == TaskState.Accepted)
                .expectedTaskStateChange(0, TaskState.Accepted)
                .expectScheduleRequest(0)
                .triggerMesosLaunchEvent(0)
                .expectedTaskStateChange(0, TaskState.Launched)
                .expectTaskUpdatedInStore(task -> task.getStatus().getState() == TaskState.Launched)
                .triggerMesosStartInitiatedEvent(0)
                .expectedTaskStateChange(0, TaskState.StartInitiated)
                .expectTaskUpdatedInStore(task -> task.getStatus().getState() == TaskState.StartInitiated)
                .triggerMesosStartedEvent(0)
                .expectedTaskStateChange(0, TaskState.Started)
                .expectTaskUpdatedInStore(task -> !task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP).isEmpty());
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> finishSingleTaskJob() {
        return jobScenario -> jobScenario
                .triggerMesosFinishedEvent(0)
                .expectedTaskStateChange(0, TaskState.Finished)
                .expectTaskUpdatedInStore(task -> task.getStatus().getState() == TaskState.Finished)
                .advance()
                .expectJobEvent(job -> job.getStatus().getState() == JobState.Finished)
                .advance()
                .expectJobUpdatedInStore(job -> job.getStatus().getState() == JobState.Finished)
                .expectedTaskArchivedInStore()
                .expectJobArchivedInStore();
    }
}
