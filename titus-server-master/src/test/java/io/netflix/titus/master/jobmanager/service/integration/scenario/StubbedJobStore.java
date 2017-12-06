/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import io.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.store.JobStore;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Pair;
import org.assertj.core.api.Assertions;
import rx.Completable;
import rx.Observable;
import rx.subjects.PublishSubject;

class StubbedJobStore implements JobStore {

    enum StoreEvent {
        JobAdded,
        JobRemoved,
        JobUpdated,
        TaskAdded,
        TaskRemoved,
        TaskUpdated,
    }

    private final PublishSubject<Pair<StoreEvent, ?>> eventSubject = PublishSubject.create();

    private final ConcurrentMap<String, Job<?>> jobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Task> tasks = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Job<?>> archivedJobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Task> archivedTasks = new ConcurrentHashMap<>();

    public Observable<Pair<StoreEvent, ?>> events() {
        return eventSubject;
    }

    public Observable<Pair<StoreEvent, ?>> events(String jobId) {
        return eventSubject
                .filter(event -> {
                    if (event.getRight() instanceof Job) {
                        Job eventJob = (Job) event.getRight();
                        return jobId.equals(eventJob.getId());
                    }
                    if (event.getRight() instanceof Task) {
                        Task eventTask = (Task) event.getRight();
                        return jobId.equals(eventTask.getJobId());
                    }
                    return false;
                });
    }

    public Task expectTaskInStore(String jobId, int taskIdx, int resubmit) {
        Optional<Task> match = findTask(jobId, taskIdx, resubmit, tasks);

        Assertions.assertThat(match)
                .describedAs("No batch task {job=%s, index=%s, resubmit=%s} found in task active store", jobId, taskIdx, resubmit)
                .isPresent();
        return match.get();
    }

    public Task expectTaskInStoreOrStoreArchive(String jobId, int taskIdx, int resubmit) {
        Optional<Task> match = findTask(jobId, taskIdx, resubmit, tasks);
        if (!match.isPresent()) {
            match = findTask(jobId, taskIdx, resubmit, archivedTasks);
        }

        Assertions.assertThat(match)
                .describedAs("No batch task {job=%s, index=%s, resubmit=%s} found in task active store or archive", jobId, taskIdx, resubmit)
                .isPresent();
        return match.get();
    }

    public Task expectTaskInStoreOrStoreArchive(String taskId) {
        Task task = tasks.getOrDefault(taskId, archivedTasks.get(taskId));

        Assertions.assertThat(task)
                .describedAs("No batch task %s found in task active store or archive", taskId)
                .isNotNull();
        return task;
    }

    public Task expectTaskInStoreArchive(String jobId, int taskIdx, int resubmit) {
        Optional<Task> match = findTask(jobId, taskIdx, resubmit, archivedTasks);

        Assertions.assertThat(match)
                .describedAs("No batch task {job=%s, index=%s, resubmit=%s} found in task archive", jobId, taskIdx, resubmit)
                .isPresent();
        return match.get();
    }

    private Optional<Task> findTask(String jobId, int taskIdx, int resubmit, Map<String, Task> taskMap) {
        return taskMap.values().stream().filter(task -> {
            if (!task.getJobId().equals(jobId)) {
                return false;
            }
            if (!(task instanceof BatchJobTask)) {
                return false;
            }
            BatchJobTask batchJobTask = (BatchJobTask) task;
            return batchJobTask.getIndex() == taskIdx && batchJobTask.getResubmitNumber() == resubmit;
        }).findFirst();
    }

    @Override
    public Completable init() {
        return Completable.complete();
    }

    @Override
    public Observable<Job<?>> retrieveJobs() {
        return ObservableExt.fromCollection(jobs::values);
    }

    @Override
    public Observable<Job<?>> retrieveJob(String jobId) {
        Callable<Job<?>> jobCallable = () -> jobs.get(jobId);
        return Observable.fromCallable(jobCallable).filter(Objects::nonNull);
    }

    @Override
    public Completable storeJob(Job job) {
        return Completable.fromAction(() -> {
            jobs.put(job.getId(), job);
            eventSubject.onNext(Pair.of(StoreEvent.JobAdded, job));
        });
    }

    @Override
    public Completable updateJob(Job job) {
        return Completable.fromAction(() -> {
            jobs.put(job.getId(), job);
            eventSubject.onNext(Pair.of(StoreEvent.JobUpdated, job));
        });
    }

    @Override
    public Completable deleteJob(Job job) {
        return Completable.fromAction(() -> {
            Job<?> removedJob = jobs.remove(job.getId());
            if (removedJob != null) {
                // We sort tasks by index, to make events more predictable for easier evaluation in test code.
                tasks.values().stream()
                        .sorted((task1, task2) -> {
                            BatchJobTask batchTask1 = (BatchJobTask) task1;
                            BatchJobTask batchTask2 = (BatchJobTask) task2;
                            return Integer.compare(batchTask1.getIndex(), batchTask2.getIndex());
                        })
                        .filter(task -> task.getJobId().equals(job.getId()))
                        .forEach(task -> {
                            tasks.remove(task.getId());
                            archivedTasks.put(task.getId(), task);
                            eventSubject.onNext(Pair.of(StoreEvent.TaskRemoved, task));
                        });
                archivedJobs.put(removedJob.getId(), removedJob);
                eventSubject.onNext(Pair.of(StoreEvent.JobRemoved, job));
            }
        });
    }

    @Override
    public Observable<Task> retrieveTasksForJob(String jobId) {
        return ObservableExt.fromCollection(() ->
                tasks.values().stream().filter(t -> t.getJobId().equals(jobId)).collect(Collectors.toList())
        );
    }

    @Override
    public Observable<Task> retrieveTask(String taskId) {
        return Observable.fromCallable(() -> tasks.get(taskId)).filter(Objects::nonNull);
    }

    @Override
    public Completable storeTask(Task task) {
        return Completable.fromAction(() -> {
            if (jobs.get(task.getJobId()) != null) {
                tasks.put(task.getId(), task);
                eventSubject.onNext(Pair.of(StoreEvent.TaskAdded, task));
            } else {
                throw new IllegalStateException("Adding task for unknown job " + task.getJobId());
            }
        });
    }

    @Override
    public Completable updateTask(Task task) {
        return Completable.fromAction(() -> {
            if (jobs.get(task.getJobId()) != null) {
                tasks.put(task.getId(), task);
                eventSubject.onNext(Pair.of(StoreEvent.TaskUpdated, task));
            } else {
                throw new IllegalStateException("Adding task for unknown job " + task.getJobId());
            }
        });
    }

    @Override
    public Completable replaceTask(Task oldTask, Task newTask) {
        return storeTask(newTask).concatWith(deleteTask(oldTask));
    }

    @Override
    public Completable deleteTask(Task task) {
        return Completable.fromAction(() -> {
            Task removedTask = tasks.remove(task.getId());
            if (removedTask != null) {
                archivedTasks.put(removedTask.getId(), removedTask);
                eventSubject.onNext(Pair.of(StoreEvent.TaskRemoved, task));
            }
        });
    }

    @Override
    public Observable<Task> retrieveArchivedTask(String taskId) {
        return Observable.fromCallable(() -> archivedTasks.get(taskId)).filter(Objects::nonNull);
    }

    @Override
    public Observable<Job<?>> retrieveArchivedJob(String jobId) {
        Callable<Job<?>> jobCallable = () -> archivedJobs.get(jobId);
        return Observable.fromCallable(jobCallable).filter(Objects::nonNull);
    }

    @Override
    public Observable<Task> retrieveArchivedTasksForJob(String jobId) {
        throw new IllegalStateException("not implemented yet");
    }
}
