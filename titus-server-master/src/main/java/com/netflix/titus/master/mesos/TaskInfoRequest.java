/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos;

import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import org.apache.mesos.Protos;

public class TaskInfoRequest {

    private final Job<?> job;
    private final Task task;
    private final Protos.TaskInfo taskInfo;
    private final Map<String, String> passthroughAttributes;

    public TaskInfoRequest(Job<?> job, Task task, Protos.TaskInfo taskInfo, Map<String, String> passthroughAttributes) {
        this.job = job;
        this.task = task;
        this.taskInfo = taskInfo;
        this.passthroughAttributes = passthroughAttributes;
    }

    public Job<?> getJob() {
        return job;
    }

    public Task getTask() {
        return task;
    }

    public Protos.TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public Map<String, String> getPassthroughAttributes() {
        return passthroughAttributes;
    }
}
