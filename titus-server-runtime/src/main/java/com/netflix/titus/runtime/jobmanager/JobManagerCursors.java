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

package com.netflix.titus.runtime.jobmanager;

import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of functions for dealing with the pagination cursors.
 */
public final class JobManagerCursors {

    private static final Logger logger = LoggerFactory.getLogger(JobManagerCursors.class);

    private static final Pattern CURSOR_RE = Pattern.compile("(.*)@(\\d+)");

    private JobManagerCursors() {
    }

    /**
     * Compare two job entities by the creation time (first), and a job id (second).
     */
    public static Comparator<Job> jobCursorOrderComparator() {
        return (first, second) -> {
            int cmp = Long.compare(getCursorTimestamp(first), getCursorTimestamp(second));
            if (cmp != 0) {
                return cmp;
            }
            return first.getId().compareTo(second.getId());
        };
    }

    /**
     * Compare two task entities by the creation time (first), and a task id (second).
     */
    public static Comparator<Task> taskCursorOrderComparator() {
        return (first, second) -> {
            int cmp = Long.compare(getCursorTimestamp(first), getCursorTimestamp(second));
            if (cmp != 0) {
                return cmp;
            }
            return first.getId().compareTo(second.getId());
        };
    }

    /**
     * Find an index of the element pointed to by the cursor, or if not found, the element immediately preceding it.
     * <p>
     * If the element pointed to by the cursor would be the first element in the list (index=0) this returns -1.
     */
    public static Optional<Integer> jobIndexOf(List<Job> jobs, String cursor) {
        return decode(cursor).map(cursorValues -> {
            String jobId = cursorValues.getLeft();
            long timestamp = cursorValues.getRight();
            Job referenceJob = Job.newBuilder()
                    .setId(jobId)
                    .setStatus(JobStatus.newBuilder().setState(JobStatus.JobState.Accepted).setTimestamp(timestamp))
                    .build();
            int idx = Collections.binarySearch(jobs, referenceJob, jobCursorOrderComparator());
            if (idx >= 0) {
                return idx;
            }
            return Math.max(-1, -idx - 2);
        });
    }

    /**
     * Find an index of the element pointed to by the cursor, or if not found, the element immediately preceding it.
     * <p>
     * If the element pointed to by the cursor would be the first element in the list (index=0) this returns -1.
     */
    public static Optional<Integer> taskIndexOf(List<Task> tasks, String cursor) {
        return decode(cursor).map(cursorValues -> {
            String taskId = cursorValues.getLeft();
            long timestamp = cursorValues.getRight();
            Task referenceTask = Task.newBuilder()
                    .setId(taskId)
                    .setStatus(TaskStatus.newBuilder().setState(TaskStatus.TaskState.Accepted).setTimestamp(timestamp))
                    .build();
            int idx = Collections.binarySearch(tasks, referenceTask, taskCursorOrderComparator());
            if (idx >= 0) {
                return idx;
            }
            return Math.max(-1, -idx - 2);
        });
    }

    public static String newCursorFrom(Job job) {
        return encode(job.getId(), getCursorTimestamp(job));
    }

    public static String newCursorFrom(Task task) {
        return encode(task.getId(), getCursorTimestamp(task));
    }

    private static long getCursorTimestamp(Job job) {
        if (job.getStatus().getState() == JobStatus.JobState.Accepted) {
            return job.getStatus().getTimestamp();
        }
        for (JobStatus next : job.getStatusHistoryList()) {
            if (next.getState() == JobStatus.JobState.Accepted) {
                return next.getTimestamp();
            }
        }
        // Fallback, in case Accepted state is not found which should never happen.
        return job.getStatus().getTimestamp();
    }

    private static long getCursorTimestamp(Task task) {
        if (task.getStatus().getState() == TaskStatus.TaskState.Accepted) {
            return task.getStatus().getTimestamp();
        }
        for (TaskStatus next : task.getStatusHistoryList()) {
            if (next.getState() == TaskStatus.TaskState.Accepted) {
                return next.getTimestamp();
            }
        }
        // Fallback, in case Accepted state is not found which should never happen.
        return task.getStatus().getTimestamp();
    }

    private static String encode(String id, long timestamp) {
        String value = id + '@' + timestamp;
        return Base64.getEncoder().encodeToString(value.getBytes());
    }

    private static Optional<Pair<String, Long>> decode(String encodedValue) {
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(encodedValue.getBytes()));
        } catch (Exception e) {
            logger.debug("Cannot decode value: {}", encodedValue, e);
            return Optional.empty();
        }

        Matcher matcher = CURSOR_RE.matcher(decoded);
        if (!matcher.matches()) {
            logger.debug("Not valid cursor value: {}", decoded);
            return Optional.empty();
        }
        String id = matcher.group(1);
        long timestamp = Long.parseLong(matcher.group(2));

        return Optional.of(Pair.of(id, timestamp));
    }
}
