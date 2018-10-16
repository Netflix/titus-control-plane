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

package com.netflix.titus.master.scheduler;

import java.util.Collections;
import java.util.List;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.titus.api.jobmanager.model.job.Task;

/**
 * Event model for scheduler placement decisions for a task.
 */
public abstract class SchedulingResultEvent {

    private final Task task;

    protected SchedulingResultEvent(Task task) {
        this.task = task;
    }

    public Task getTask() {
        return task;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "task=" + task +
                '}';
    }

    public static SuccessfulSchedulingResultEvent onStarted(Task task) {
        return new SuccessfulSchedulingResultEvent(task);
    }

    public static FailedSchedulingResultEvent onFailure(Task task, List<TaskAssignmentResult> assignmentResults) {
        return new FailedSchedulingResultEvent(task, assignmentResults);
    }

    public static FailedSchedulingResultEvent onNoAgent(Task task) {
        return new FailedSchedulingResultEvent(task, Collections.emptyList());
    }

    public static class SuccessfulSchedulingResultEvent extends SchedulingResultEvent {
        private SuccessfulSchedulingResultEvent(Task task) {
            super(task);
        }
    }

    public static class FailedSchedulingResultEvent extends SchedulingResultEvent {

        private final List<TaskAssignmentResult> assignmentResults;

        private FailedSchedulingResultEvent(Task task, List<TaskAssignmentResult> assignmentResults) {
            super(task);
            this.assignmentResults = assignmentResults;
        }

        public List<TaskAssignmentResult> getAssignmentResults() {
            return assignmentResults;
        }

        @Override
        public String toString() {
            return "FailedSchedulingResultEvent{" +
                    "task=" + getTask() +
                    ", assignmentResults=" + assignmentResults +
                    "} ";
        }
    }
}
