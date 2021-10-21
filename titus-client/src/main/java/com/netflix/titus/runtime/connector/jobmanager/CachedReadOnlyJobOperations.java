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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshot;
import reactor.core.publisher.Flux;
import rx.Observable;

@Singleton
public class CachedReadOnlyJobOperations implements ReadOnlyJobOperations {

    private final JobDataReplicator replicator;

    @Inject
    public CachedReadOnlyJobOperations(JobDataReplicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public List<Job> getJobs() {
        return (List) replicator.getCurrent().getJobs();
    }

    @Override
    public Optional<Job<?>> getJob(String jobId) {
        return replicator.getCurrent().findJob(jobId);
    }

    @Override
    public List<Task> getTasks() {
        return replicator.getCurrent().getTasks();
    }

    @Override
    public List<Task> getTasks(String jobId) {
        return new ArrayList<>(replicator.getCurrent().getTasks(jobId).values());
    }

    @Override
    public List<Pair<Job, List<Task>>> getJobsAndTasks() {
        return (List) replicator.getCurrent().getJobsAndTasks();
    }

    @Override
    public List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit) {
        JobSnapshot snapshot = replicator.getCurrent();

        return snapshot.getJobMap().values().stream()
                .filter(job -> queryPredicate.test(Pair.of(job, new ArrayList<>(snapshot.getTasks(job.getId()).values()))))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
        JobSnapshot snapshot = replicator.getCurrent();

        return snapshot.getJobMap().values().stream()
                .flatMap(job -> snapshot.getTasks(job.getId()).values().stream()
                        .filter(task -> queryPredicate.test(Pair.of(job, task)))
                        .map(task -> Pair.<Job<?>, Task>of(job, task))
                )
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
        return replicator.getCurrent().findTaskById(taskId);
    }

    /**
     * TODO Emit snapshot on subscription
     * TODO Handle failover scenarios (onError or make full snapshot diff)
     */
    @Override
    @Experimental(deadline = "03/2019")
    public Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate, Predicate<Pair<Job<?>, Task>> tasksPredicate) {
        Flux<JobManagerEvent<?>> fluxStream = replicator.events()
                .filter(event -> {
                    if (event.getRight() instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event.getRight();
                        Job<?> job = jobUpdateEvent.getCurrent();
                        List<Task> tasks = new ArrayList<>(replicator.getCurrent().getTasks(job.getId()).values());
                        return jobsPredicate.test(Pair.of(job, tasks));
                    }
                    if (event.getRight() instanceof TaskUpdateEvent) {
                        TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event.getRight();
                        return tasksPredicate.test(Pair.of(taskUpdateEvent.getCurrentJob(), taskUpdateEvent.getCurrentTask()));
                    }
                    return false;
                })
                .map(Pair::getRight);
        return ReactorExt.toObservable(fluxStream);
    }

    /**
     * TODO Handle case when job is not found or there is a reconnect during which time the job is terminated.
     */
    @Override
    @Experimental(deadline = "03/2019")
    public Observable<JobManagerEvent<?>> observeJob(String jobId) {
        return observeJobs(jobTasks -> jobTasks.getLeft().getId().equals(jobId), jobTask -> jobTask.getLeft().getId().equals(jobId))
                .takeUntil(event -> {
                    if (event instanceof JobUpdateEvent) {
                        JobUpdateEvent jobUpdateEvent = (JobUpdateEvent) event;
                        return jobUpdateEvent.getCurrent().getStatus().getState() == JobState.Finished;
                    }
                    return false;
                });
    }
}
