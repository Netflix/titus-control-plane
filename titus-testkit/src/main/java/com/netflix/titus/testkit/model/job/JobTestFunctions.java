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

package com.netflix.titus.testkit.model.job;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public final class JobTestFunctions {

    private JobTestFunctions() {
    }

    public static Map<String, Job<?>> toJobMap(Collection<Job> jobs) {
        return jobs.stream().collect(Collectors.toMap(Job::getId, j -> j));
    }

    public static Map<String, Task> toTaskMap(Collection<Task> tasks) {
        return tasks.stream().collect(Collectors.toMap(Task::getId, t -> t));
    }
}
