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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.assertj.core.api.Assertions;
import rx.Completable;
import rx.Observable;
import rx.subjects.PublishSubject;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.isServiceJob;

class StubbedJobStore implements JobStore {

    enum StoreEvent {
        JobAdded,
        JobRemoved,
        JobUpdated,
        TaskAdded,
        TaskRemoved,
        TaskUpdated,
    }

    private boolean broken;

    private final PublishSubject<Pair<StoreEvent, ?>> eventSubject = PublishSubject.create();

    private final ConcurrentMap<String, Job<?>> jobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Task> tasks = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Job<?>> archivedJobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Task> archivedTasks = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, ServiceTaskIndex> jobToServiceTaskIndex = new ConcurrentHashMap<>();

    public void setBroken(boolean broken) {
        this.broken = broken;
    }

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

    public int getIndex(String taskId) {
        return getIndexAndResubmit(taskId).map(p -> p.getLeft()).orElseThrow(() -> new IllegalStateException("Task " + taskId + " is not registered in store"));
    }

    public Optional<Pair<Integer, Integer>> getIndexAndResubmit(String taskId) {
        Task task = tasks.getOrDefault(taskId, archivedTasks.get(taskId));
        if (task == null) {
            return Optional.empty();
        }
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            return Optional.of(Pair.of(batchJobTask.getIndex(), task.getResubmitNumber()));
        }
        ServiceTaskIndex serviceTaskIndex = jobToServiceTaskIndex.get(task.getJobId());
        if (serviceTaskIndex == null) {
            return Optional.empty();
        }
        return serviceTaskIndex.getTaskIndexAndResubmitById(taskId);
    }

    public boolean hasIndexAndResubmit(Task task, int taskIdx, int resubmit) {
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            return batchJobTask.getIndex() == taskIdx && batchJobTask.getResubmitNumber() == resubmit;
        }
        ServiceTaskIndex serviceTaskIndex = jobToServiceTaskIndex.get(task.getJobId());
        if (serviceTaskIndex == null) {
            return false;
        }
        return serviceTaskIndex.getTaskIdByIndexAndResubmit(taskIdx, resubmit)
                .map(foundTaskId -> foundTaskId.equals(task.getId()))
                .orElse(false);
    }

    public Task expectTaskInStore(String jobId, int taskIdx, int resubmit) {
        Optional<Task> match = findTask(jobId, taskIdx, resubmit, tasks);

        Assertions.assertThat(match)
                .describedAs("No task {job=%s, index=%s, resubmit=%s} found in task active store", jobId, taskIdx, resubmit)
                .isPresent();
        return match.get();
    }

    public Task expectTaskInStoreOrStoreArchive(String jobId, int taskIdx, int resubmit) {
        Optional<Task> match = findTask(jobId, taskIdx, resubmit, tasks);
        if (!match.isPresent()) {
            match = findTask(jobId, taskIdx, resubmit, archivedTasks);
        }

        Assertions.assertThat(match)
                .describedAs("No task {job=%s, index=%s, resubmit=%s} found in task active store or archive", jobId, taskIdx, resubmit)
                .isPresent();
        return match.get();
    }

    public Task expectTaskInStoreOrStoreArchive(String taskId) {
        Task task = tasks.getOrDefault(taskId, archivedTasks.get(taskId));

        Assertions.assertThat(task)
                .describedAs("No task %s found in task active store or archive", taskId)
                .isNotNull();
        return task;
    }

    public Task expectTaskInStoreArchive(String jobId, int taskIdx, int resubmit) {
        Optional<Task> match = findTask(jobId, taskIdx, resubmit, archivedTasks);

        Assertions.assertThat(match)
                .describedAs("No task {job=%s, index=%s, resubmit=%s} found in task archive", jobId, taskIdx, resubmit)
                .isPresent();
        return match.get();
    }

    private Optional<Task> findTask(String jobId, int taskIdx, int resubmit, Map<String, Task> taskMap) {
        Job<?> job = jobs.getOrDefault(jobId, archivedJobs.get(jobId));
        if (JobFunctions.isBatchJob(job)) {
            return taskMap.values().stream().filter(task -> {
                if (!task.getJobId().equals(jobId)) {
                    return false;
                }
                BatchJobTask batchJobTask = (BatchJobTask) task;
                return batchJobTask.getIndex() == taskIdx && batchJobTask.getResubmitNumber() == resubmit;
            }).findFirst();
        }

        // For service job we need to use our internal index
        ServiceTaskIndex serviceTaskIndex = jobToServiceTaskIndex.get(jobId);
        if (serviceTaskIndex == null) {
            return Optional.empty();
        }
        return serviceTaskIndex.getTaskIdByIndexAndResubmit(taskIdx, resubmit)
                .flatMap(taskId ->
                        taskMap.values().stream()
                                .filter(task -> {
                                    if (!task.getJobId().equals(jobId)) {
                                        return false;
                                    }
                                    return task.getId().equals(taskId);
                                })
                                .findFirst()
                );
    }

    @Override
    public Completable init() {
        return Completable.complete();
    }

    @Override
    public Observable<Pair<List<Job<?>>, Integer>> retrieveJobs() {
        return beforeObservable(() -> Observable.just(Pair.of(new ArrayList<>(jobs.values()), 0)));
    }

    @Override
    public Observable<Job<?>> retrieveJob(String jobId) {
        return beforeObservable(() -> {
            Callable<Job<?>> jobCallable = () -> jobs.get(jobId);
            return Observable.fromCallable(jobCallable).filter(Objects::nonNull);
        });
    }

    @Override
    public Completable storeJob(Job job) {
        return beforeCompletable(() ->
                Completable.fromAction(() -> {
                    jobs.put(job.getId(), job);
                    if (isServiceJob(job)) {
                        jobToServiceTaskIndex.put(job.getId(), new ServiceTaskIndex());
                    }
                    eventSubject.onNext(Pair.of(StoreEvent.JobAdded, job));
                }));
    }

    @Override
    public Completable updateJob(Job job) {
        return beforeCompletable(() ->
                Completable.fromAction(() -> {
                    jobs.put(job.getId(), job);
                    eventSubject.onNext(Pair.of(StoreEvent.JobUpdated, job));
                }));
    }

    @Override
    public Completable deleteJob(Job job) {
        return beforeCompletable(() ->
                Completable.fromAction(() -> {
                    Job<?> removedJob = jobs.remove(job.getId());
                    if (removedJob != null) {
                        // We sort tasks by index, to make events more predictable for easier evaluation in test code.
                        tasks.values().stream()
                                .sorted(Comparator.comparingInt(task2 -> getIndex(task2.getId())))
                                .filter(task -> task.getJobId().equals(job.getId()))
                                .forEach(task -> {
                                    tasks.remove(task.getId());
                                    archivedTasks.put(task.getId(), task);
                                    eventSubject.onNext(Pair.of(StoreEvent.TaskRemoved, task));
                                });
                        archivedJobs.put(removedJob.getId(), removedJob);
                        eventSubject.onNext(Pair.of(StoreEvent.JobRemoved, job));
                    }
                }));
    }

    @Override
    public Observable<Pair<List<Task>, Integer>> retrieveTasksForJob(String jobId) {
        return beforeObservable(() ->
                ObservableExt.fromCallable(() -> {
                            List<Task> jobTasks = tasks.values().stream().filter(t -> t.getJobId().equals(jobId)).collect(Collectors.toList());
                            return Collections.singletonList(Pair.of(jobTasks, 0));
                        }
                ));
    }

    @Override
    public Observable<Task> retrieveTask(String taskId) {
        return beforeObservable(() ->
                Observable.fromCallable(() -> tasks.get(taskId)).filter(Objects::nonNull)
        );
    }

    @Override
    public Completable storeTask(Task task) {
        return beforeCompletable(() ->
                Completable.fromAction(() -> {
                    Job<?> job = jobs.get(task.getJobId());
                    if (job != null) {
                        tasks.put(task.getId(), task);

                        if (isServiceJob(job)) {
                            jobToServiceTaskIndex.get(job.getId()).addTask(task);
                        }

                        eventSubject.onNext(Pair.of(StoreEvent.TaskAdded, task));
                    } else {
                        throw new IllegalStateException("Adding task for unknown job " + task.getJobId());
                    }
                }));
    }

    @Override
    public Completable updateTask(Task task) {
        return beforeCompletable(() ->
                Completable.fromAction(() -> {
                    if (jobs.get(task.getJobId()) != null) {
                        tasks.put(task.getId(), task);
                        eventSubject.onNext(Pair.of(StoreEvent.TaskUpdated, task));
                    } else {
                        throw new IllegalStateException("Adding task for unknown job " + task.getJobId());
                    }
                }));
    }

    @Override
    public Completable replaceTask(Task oldTask, Task newTask) {
        return beforeCompletable(() ->
                storeTask(newTask).concatWith(deleteTask(oldTask))
        );
    }

    @Override
    public Completable deleteTask(Task task) {
        return beforeCompletable(() ->
                Completable.fromAction(() -> {
                    Task removedTask = tasks.remove(task.getId());
                    if (removedTask != null) {
                        archivedTasks.put(removedTask.getId(), removedTask);
                        eventSubject.onNext(Pair.of(StoreEvent.TaskRemoved, task));
                    }
                }));
    }

    @Override
    public Observable<Task> retrieveArchivedTask(String taskId) {
        return beforeObservable(() -> Observable.fromCallable(() -> archivedTasks.get(taskId)).filter(Objects::nonNull));
    }

    @Override
    public Observable<Job<?>> retrieveArchivedJob(String jobId) {
        return beforeObservable(() -> {
            Callable<Job<?>> jobCallable = () -> archivedJobs.get(jobId);
            return Observable.fromCallable(jobCallable).filter(Objects::nonNull);
        });
    }

    @Override
    public Observable<Task> retrieveArchivedTasksForJob(String jobId) {
        throw new IllegalStateException("not implemented yet");
    }

    private Completable beforeCompletable(Supplier<Completable> action) {
        if (broken) {
            return Completable.error(new IOException("Store is broken"));
        }
        return action.get();
    }

    private <R> Observable<R> beforeObservable(Supplier<Observable<R>> action) {
        if (broken) {
            return Observable.error(new IOException("Store is broken"));
        }
        return action.get();
    }

    /**
     * Service tasks contain no longer index. To simplify task access we assign index equivalent to each newly added task
     * (resubmitted task reuses index assigned to the original task).
     */
    private static class ServiceTaskIndex {

        private int nextIdx;
        private Map<Integer, List<String>> taskIds = new HashMap<>();

        private void addTask(Task task) {
            String originalId = task.getOriginalId();
            Optional<Pair<Integer, Integer>> taskIndexAndResubmit = getTaskIndexAndResubmitById(originalId);
            if (taskIndexAndResubmit.isPresent()) {
                List<String> ids = taskIds.get(taskIndexAndResubmit.get().getLeft());
                Preconditions.checkArgument(ids.indexOf(task.getId()) == -1, "Task with id %s has been already created", task.getId());
                ids.add(task.getId());
            } else {
                List<String> ids = new ArrayList<>();
                ids.add(task.getId());
                taskIds.put(nextIdx++, ids);
            }
        }

        private Optional<Pair<Integer, Integer>> getTaskIndexAndResubmitById(String taskId) {
            for (Map.Entry<Integer, List<String>> entry : taskIds.entrySet()) {
                int taskIndex = entry.getKey();
                int resubmit = entry.getValue().indexOf(taskId);
                if (resubmit >= 0) {
                    return Optional.of(Pair.of(taskIndex, resubmit));
                }
            }
            return Optional.empty();
        }

        private Optional<String> getTaskIdByIndexAndResubmit(int taskIdx, int resubmit) {
            List<String> slotIds = taskIds.get(taskIdx);
            if (slotIds != null && slotIds.size() > resubmit) {
                return Optional.of(slotIds.get(resubmit));
            }
            return Optional.empty();
        }
    }
}
