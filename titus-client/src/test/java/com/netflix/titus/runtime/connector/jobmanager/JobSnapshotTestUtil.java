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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.model.job.JobGenerator;

class JobSnapshotTestUtil {

    static JobSnapshot newSnapshot(JobSnapshotFactory factory, Pair<Job<?>, List<Task>>... pairs) {
        Map<String, Job<?>> jobsById = new HashMap<>();
        Map<String, List<Task>> tasksByJobId = new HashMap<>();
        for (Pair<Job<?>, List<Task>> pair : pairs) {
            Job<?> job = pair.getLeft();
            jobsById.put(job.getId(), job);
            tasksByJobId.put(job.getId(), pair.getRight());
        }
        return factory.newSnapshot(jobsById, tasksByJobId);
    }

    static Pair<Job<?>, List<Task>> newJobWithTasks(int jobIdx, int taskCount) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob().toBuilder().withId("job#" + jobIdx).build();
        List<Task> tasks = new ArrayList<>();
        for (int t = 0; t < taskCount; t++) {
            tasks.add(newTask(job, t));
        }
        return Pair.of(job, tasks);
    }

    static BatchJobTask newTask(Job<?> job, int taskIdx) {
        return JobGenerator.oneBatchTask().toBuilder()
                .withId("task#" + taskIdx + "@" + job.getId())
                .withJobId(job.getId())
                .build();
    }
}
