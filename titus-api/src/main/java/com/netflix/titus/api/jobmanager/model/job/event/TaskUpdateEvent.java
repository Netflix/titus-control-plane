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

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public class TaskUpdateEvent extends JobManagerEvent<Task> {

    private final Job<?> currentJob;
    private final Task currentTask;
    private final Optional<Task> previousTask;

    private TaskUpdateEvent(Job<?> currentJob, Task currentTask, Optional<Task> previousTask) {
        super(currentTask, previousTask);
        this.currentJob = currentJob;
        this.currentTask = currentTask;
        this.previousTask = previousTask;
    }

    public Job<?> getCurrentJob() {
        return currentJob;
    }

    public Task getCurrentTask() {
        return currentTask;
    }

    public Optional<Task> getPreviousTask() {
        return previousTask;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TaskUpdateEvent that = (TaskUpdateEvent) o;

        return currentJob != null ? currentJob.equals(that.currentJob) : that.currentJob == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (currentJob != null ? currentJob.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TaskUpdateEvent{" +
                "currentJob=" + currentJob +
                ", current=" + getCurrent() +
                ", previous=" + getPrevious() +
                "}";
    }

    public static TaskUpdateEvent newTask(Job job, Task current) {
        return new TaskUpdateEvent(job, current, Optional.empty());
    }

    public static TaskUpdateEvent taskChange(Job job, Task current, Task previous) {
        return new TaskUpdateEvent(job, current, Optional.of(previous));
    }
}
