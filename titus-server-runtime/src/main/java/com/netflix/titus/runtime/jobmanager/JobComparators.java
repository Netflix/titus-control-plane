/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.jobmanager;

import java.util.Comparator;
import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.model.IdAndTimestampKey;

public class JobComparators {

    private static final JobTimestampComparator JOB_TIMESTAMP_COMPARATOR = new JobTimestampComparator();

    private static final TaskTimestampComparator TASK_TIMESTAMP_COMPARATOR = new TaskTimestampComparator();

    /**
     * Compare two job entities by the creation time (first), and a job id (second).
     */
    public static Comparator<Job<?>> getJobTimestampComparator() {
        return JOB_TIMESTAMP_COMPARATOR;
    }

    /**
     * Compare two task entities by the creation time (first), and a task id (second).
     */
    public static Comparator<Task> getTaskTimestampComparator() {
        return TASK_TIMESTAMP_COMPARATOR;
    }

    private static class JobTimestampComparator implements Comparator<Job<?>> {
        @Override
        public int compare(Job first, Job second) {
            long firstTimestamp = getJobCreateTimestamp(first);
            long secondTimestamp = getJobCreateTimestamp(second);
            int cmp = Long.compare(firstTimestamp, secondTimestamp);
            if (cmp != 0) {
                return cmp;
            }
            return first.getId().compareTo(second.getId());
        }
    }

    private static class TaskTimestampComparator implements Comparator<Task> {
        @Override
        public int compare(Task first, Task second) {
            long firstTimestamp = getTaskCreateTimestamp(first);
            long secondTimestamp = getTaskCreateTimestamp(second);
            int cmp = Long.compare(firstTimestamp, secondTimestamp);
            if (cmp != 0) {
                return cmp;
            }
            return first.getId().compareTo(second.getId());
        }
    }

    public static long getJobCreateTimestamp(Job job) {
        List<JobStatus> statusHistory = job.getStatusHistory();
        int historySize = statusHistory.size();

        // Fast path
        if (historySize == 0) {
            return job.getStatus().getTimestamp();
        }
        JobStatus first = statusHistory.get(0);
        if (historySize == 1 || first.getState() == JobState.Accepted) {
            return first.getTimestamp();
        }
        // Slow path
        for (int i = 1; i < historySize; i++) {
            JobStatus next = statusHistory.get(i);
            if (next.getState() == JobState.Accepted) {
                return next.getTimestamp();
            }
        }
        // No Accepted state
        return job.getStatus().getTimestamp();
    }

    public static long getTaskCreateTimestamp(Task task) {
        List<TaskStatus> statusHistory = task.getStatusHistory();
        int historySize = statusHistory.size();

        // Fast path
        if (historySize == 0) {
            return task.getStatus().getTimestamp();
        }
        com.netflix.titus.api.jobmanager.model.job.TaskStatus first = statusHistory.get(0);
        if (historySize == 1 || first.getState() == TaskState.Accepted) {
            return first.getTimestamp();
        }
        // Slow path
        for (int i = 1; i < historySize; i++) {
            com.netflix.titus.api.jobmanager.model.job.TaskStatus next = statusHistory.get(i);
            if (next.getState() == TaskState.Accepted) {
                return next.getTimestamp();
            }
        }
        // No Accepted state
        return task.getStatus().getTimestamp();
    }

    public static IdAndTimestampKey<Job<?>> createJobKeyOf(Job<?> job) {
        return new IdAndTimestampKey<>(job, job.getId(), getJobCreateTimestamp(job));
    }

    public static IdAndTimestampKey<Task> createTaskKeyOf(Task task) {
        return new IdAndTimestampKey<>(task, task.getId(), getTaskCreateTimestamp(task));
    }
}
