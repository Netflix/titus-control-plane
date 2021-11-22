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

package com.netflix.titus.cli.command.job;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.cli.CommandContext;
import com.netflix.titus.common.util.tuple.Pair;

public class JobUtil {

    public static Pair<Map<String, Job>, Map<String, Map<String, Task>>> loadActiveJobsAndTasks(CommandContext context) {
        Map<String, Job> activeJobs = new HashMap<>();
        Map<String, Map<String, Task>> activeTasks = new HashMap<>();
        Iterator<JobManagerEvent<?>> it = context.getJobManagementClient().observeJobs(Collections.emptyMap())
                .toIterable()
                .iterator();
        while (it.hasNext()) {
            JobManagerEvent<?> event = it.next();
            if (event instanceof JobUpdateEvent) {
                JobUpdateEvent je = (JobUpdateEvent) event;
                Job job = je.getCurrent();
                if (job.getStatus().getState() == JobState.Accepted) {
                    activeJobs.put(job.getId(), job);
                }
            } else if (event instanceof TaskUpdateEvent) {
                TaskUpdateEvent te = (TaskUpdateEvent) event;
                Task task = te.getCurrent();
                if (activeJobs.containsKey(task.getJobId())) {
                    activeTasks.computeIfAbsent(task.getJobId(), j -> new HashMap<>()).put(task.getId(), task);
                }
            } else if (event.equals(JobManagerEvent.snapshotMarker())) {
                break;
            }
        }
        return Pair.of(activeJobs, activeTasks);
    }
}
