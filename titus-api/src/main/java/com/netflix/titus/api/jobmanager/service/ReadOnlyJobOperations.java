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

package com.netflix.titus.api.jobmanager.service;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.ContainerState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Flux;
import rx.Observable;

import static com.netflix.titus.common.util.FunctionExt.alwaysTrue;

/**
 * An interface providing the read-only view into the job data.
 */
public interface ReadOnlyJobOperations {

    List<Job> getJobs();

    Optional<Job<?>> getJob(String jobId);

    List<Task> getTasks();

    List<Task> getTasks(String jobId);

    List<Pair<Job, List<Task>>> getJobsAndTasks();

    List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit);

    List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit);

    Optional<Pair<Job<?>, Task>> findTaskById(String taskId);

    default Observable<JobManagerEvent<?>> observeJobs() {
        return observeJobs(alwaysTrue(), alwaysTrue(), false);
    }

    Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                               Predicate<Pair<Job<?>, Task>> tasksPredicate,
                                               boolean withCheckpoints);

    Observable<JobManagerEvent<?>> observeJob(String jobId);

    default Flux<JobManagerEvent<?>> observeJobsReactor() {
        return ReactorExt.toFlux(observeJobs(alwaysTrue(), alwaysTrue(), false));
    }

    default Flux<JobManagerEvent<?>> observeJobsReactor(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                                        Predicate<Pair<Job<?>, Task>> tasksPredicate) {
        return ReactorExt.toFlux(observeJobs(jobsPredicate, tasksPredicate, false));
    }

    default Flux<JobManagerEvent<?>> observeJobReactor(String jobId) {
        return ReactorExt.toFlux(observeJob(jobId));
    }

    Optional<List<ContainerState>> findEphemeralTaskStatus(String taskId);
}
