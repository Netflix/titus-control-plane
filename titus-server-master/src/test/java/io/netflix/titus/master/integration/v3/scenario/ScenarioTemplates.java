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

package io.netflix.titus.master.integration.v3.scenario;

import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.grpc.protogen.TaskStatus.TaskState;
import io.netflix.titus.api.jobmanager.model.job.JobState;

/**
 */
public class ScenarioTemplates {

    public static Function<JobScenarioBuilder, JobScenarioBuilder> jobAccepted() {
        return jobScenarioBuilder -> jobScenarioBuilder.expectJobUpdateEvent(
                job -> job.getStatus().getState() == JobState.Accepted, "Expected state: " + JobState.Accepted
        );
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> jobFinished() {
        return jobScenarioBuilder -> jobScenarioBuilder.expectJobUpdateEvent(
                job -> job.getStatus().getState() == JobState.Finished, "Expected state: " + JobState.Finished
        );
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> launchJob() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .template(jobAccepted())
                .expectAllTasksCreated()
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskStatus.TaskState.Launched));
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> startJob(TaskState taskState) {
        Preconditions.checkArgument(taskState.ordinal() < TaskState.KillInitiated.ordinal(), "Invalid target task state: %s", taskState);
        return jobScenarioBuilder -> jobScenarioBuilder
                .template(jobAccepted())
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .allTasks(moveToState(taskState));
    }

    /**
     * KillInitiated state is volatile. To force a task to stay in it forever, set lock = true.
     */
    public static Function<JobScenarioBuilder, JobScenarioBuilder> startJobAndMoveTasksToKillInitiated(boolean lock) {
        return jobScenarioBuilder -> {
            jobScenarioBuilder.template(startJob(TaskState.Started));

            if (lock) {
                jobScenarioBuilder.allTasks(taskScenarioBuilder -> {
                    taskScenarioBuilder.getTaskExecutionHolder().delayStateTransition(state -> Long.MAX_VALUE);
                    return taskScenarioBuilder;
                });
                return jobScenarioBuilder.allTasks(taskScenarioBuilder -> taskScenarioBuilder
                        .killTask()
                        .expectStateUpdates(TaskState.KillInitiated)
                );
            }

            return jobScenarioBuilder.allTasks(taskScenarioBuilder -> taskScenarioBuilder
                    .killTask()
                    .expectStateUpdates(TaskState.KillInitiated, TaskState.Finished)
            );
        };
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> startTasks() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Launched))
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.transitionTo(TaskState.StartInitiated, TaskState.Started))
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.StartInitiated, TaskState.Started));
    }

    public static Function<TaskScenarioBuilder, TaskScenarioBuilder> startTask() {
        return taskScenarioBuilder -> taskScenarioBuilder
                .expectStateUpdateSkipOther(TaskState.Launched)
                .transitionTo(TaskState.StartInitiated, TaskState.Started)
                .expectStateUpdates(TaskState.StartInitiated, TaskState.Started);
    }

    /**
     * V2 engine emits fewer events when task is created/scheduled, so we have to handle this case separately.
     */
    public static Function<JobScenarioBuilder, JobScenarioBuilder> startV2Tasks() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.transitionTo(TaskState.StartInitiated, TaskState.Started))
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdateSkipOther(TaskState.Started));
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> killJob() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .killJob()
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.KillInitiated, "Expected state: " + JobState.KillInitiated)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.KillInitiated, TaskState.Finished))
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.Finished, "Expected state: " + JobState.Finished)
                .expectJobEventStreamCompletes();
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> killV2Job() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .killJob()
                .expectJobUpdateEvent(job -> job.getStatus().getState() == JobState.Finished, "Expected state: " + JobState.Finished)
                .allTasks(taskScenarioBuilder -> taskScenarioBuilder.expectStateUpdates(TaskState.Finished))
                .expectJobEventStreamCompletes();
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> startTasksInNewJob() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .template(jobAccepted())
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .template(startTasks());
    }

    public static Function<JobScenarioBuilder, JobScenarioBuilder> startV2TasksInNewJob() {
        return jobScenarioBuilder -> jobScenarioBuilder
                .template(jobAccepted())
                .expectAllTasksCreated()
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .template(startV2Tasks());
    }

    public static Function<TaskScenarioBuilder, TaskScenarioBuilder> moveToState(TaskState taskState) {
        return taskScenarioBuilder -> taskScenarioBuilder
                .transitionUntil(taskState)
                .expectStateUpdateSkipOther(taskState);
    }


    public static Function<TaskScenarioBuilder, TaskScenarioBuilder> completeTask() {
        return taskScenarioBuilder -> taskScenarioBuilder
                .transitionTo(TaskState.Finished)
                .expectStateUpdateSkipOther(TaskState.Finished);
    }

    public static Function<TaskScenarioBuilder, TaskScenarioBuilder> lockTaskInState(TaskStatus.TaskState lockedState) {
        return taskScenarioBuilder -> taskScenarioBuilder
                .transitionUntil(lockedState)
                .expectStateUpdateSkipOther(lockedState)
                .expectStateUpdates(TaskState.KillInitiated, TaskState.Finished);
    }

    public static Function<TaskScenarioBuilder, TaskScenarioBuilder> terminateAndShrinkV2() {
        return taskScenarioBuilder -> taskScenarioBuilder
                .killTaskAndShrink()
                .expectStateUpdateSkipOther(TaskState.Finished);
    }

    public static Function<TaskScenarioBuilder, TaskScenarioBuilder> terminateAndShrinkV3() {
        return taskScenarioBuilder -> taskScenarioBuilder
                .killTaskAndShrink()
                .expectStateUpdateSkipOther(TaskState.KillInitiated)
                .expectStateUpdateSkipOther(TaskState.Finished);
    }
}
