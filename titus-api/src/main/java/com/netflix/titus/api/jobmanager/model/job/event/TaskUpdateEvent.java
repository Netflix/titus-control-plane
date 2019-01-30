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

package com.netflix.titus.api.jobmanager.model.job.event;

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public class TaskUpdateEvent extends JobManagerEvent<Task> {

    private final Job<?> currentJob;
    private final boolean movedFromAnotherJob;

    private TaskUpdateEvent(Job<?> currentJob, Task currentTask, Optional<Task> previousTask) {
        this(currentJob, currentTask, previousTask, false);
    }

    private TaskUpdateEvent(Job<?> currentJob, Task currentTask, Optional<Task> previousTask, boolean moved) {
        super(currentTask, previousTask);
        this.currentJob = currentJob;
        this.movedFromAnotherJob = moved;
    }

    public Job<?> getCurrentJob() {
        return currentJob;
    }

    public Task getCurrentTask() {
        return getCurrent();
    }

    public Optional<Task> getPreviousTask() {
        return getPrevious();
    }

    public boolean isMovedFromAnotherJob() {
        return movedFromAnotherJob;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TaskUpdateEvent)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TaskUpdateEvent that = (TaskUpdateEvent) o;
        return movedFromAnotherJob == that.movedFromAnotherJob &&
                currentJob.equals(that.currentJob);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), currentJob, movedFromAnotherJob);
    }

    @Override
    public String toString() {
        return "TaskUpdateEvent{" +
                "currentJob=" + currentJob +
                ", currentTask=" + getCurrent() +
                ", previousTask=" + getPrevious() +
                ", movedFromAnotherJob=" + movedFromAnotherJob +
                '}';
    }

    public static TaskUpdateEvent newTask(Job job, Task current) {
        return new TaskUpdateEvent(job, current, Optional.empty());
    }

    public static TaskUpdateEvent newTaskFromAnotherJob(Job job, Task current) {
        return new TaskUpdateEvent(job, current, Optional.empty(), true);
    }

    public static TaskUpdateEvent taskChange(Job job, Task current, Task previous) {
        return new TaskUpdateEvent(job, current, Optional.of(previous));
    }
}
