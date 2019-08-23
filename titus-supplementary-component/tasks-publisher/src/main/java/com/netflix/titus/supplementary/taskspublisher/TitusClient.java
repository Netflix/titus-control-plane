/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.supplementary.taskspublisher;

import com.google.common.base.Preconditions;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TitusClient {

    Mono<Job> getJobById(String jobId);

    Mono<Task> getTask(String taskId);

    Flux<JobOrTaskUpdate> getJobAndTaskUpdates();

    class JobOrTaskUpdate {

        private final Job job;
        private final Task task;

        private JobOrTaskUpdate(Job job, Task task) {
            this.job = job;
            this.task = task;
        }

        public Job getJob() {
            Preconditions.checkState(job != null, "Task container");
            return job;
        }

        public Task getTask() {
            Preconditions.checkState(task != null, "Job container");
            return task;
        }

        public boolean hasJob() {
            return job != null;
        }

        public boolean hasTask() {
            return task != null;
        }

        public static JobOrTaskUpdate jobUpdate(Job job) {
            Preconditions.checkNotNull(job, "Null job");
            return new JobOrTaskUpdate(job, null);
        }

        public static JobOrTaskUpdate taskUpdate(Task task) {
            Preconditions.checkNotNull(task, "Null task");
            return new JobOrTaskUpdate(null, task);
        }
    }
}
