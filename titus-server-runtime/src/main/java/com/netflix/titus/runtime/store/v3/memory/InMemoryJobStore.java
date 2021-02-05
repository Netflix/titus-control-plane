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

package com.netflix.titus.runtime.store.v3.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Singleton;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.jobmanager.store.JobStoreException;
import com.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Observable;

@Singleton
public class InMemoryJobStore implements JobStore {

    private static final int MAX_CACHE_SIZE = 100_000;

    private final Cache<String, Job<?>> jobs = CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .build();
    private final Cache<String, Task> tasks = CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .build();
    private final Cache<String, Job<?>> archivedJobs = CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .build();
    private final Cache<String, Task> archivedTasks = CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .build();

    @Override
    public Completable init() {
        return Completable.complete();
    }

    @Override
    public Observable<Pair<List<Job<?>>, Integer>> retrieveJobs() {
        return Observable.just(Pair.of(new ArrayList<>(jobs.asMap().values()), 0));
    }

    @Override
    public Observable<Job<?>> retrieveJob(String jobId) {
        return Observable.fromCallable(() -> {
            Job job = jobs.getIfPresent(jobId);
            if (job == null) {
                throw JobStoreException.jobDoesNotExist(jobId);
            }
            return job;
        });
    }

    @Override
    public Completable storeJob(Job job) {
        return Completable.fromAction(() -> jobs.put(job.getId(), job));
    }

    @Override
    public Completable updateJob(Job job) {
        return storeJob(job);
    }

    @Override
    public Completable deleteJob(Job job) {
        return Completable.fromAction(() -> {
            tasks.asMap().values().stream().filter(task -> task.getJobId().equals(job.getId())).forEach(task -> {
                tasks.invalidate(task.getId());
                archivedTasks.put(task.getId(), task);
            });
            jobs.invalidate(job.getId());
            archivedJobs.put(job.getId(), job);
        });
    }

    @Override
    public Observable<Pair<List<Task>, Integer>> retrieveTasksForJob(String jobId) {
        List<Task> jobTask = tasks.asMap().values().stream()
                .filter(task -> task.getJobId().equals(jobId))
                .collect(Collectors.toList());
        return Observable.just(Pair.of(jobTask, 0));
    }

    @Override
    public Observable<Task> retrieveTask(String taskId) {
        return Observable.fromCallable(() -> {
            Task task = tasks.getIfPresent(taskId);
            if (task == null) {
                throw JobStoreException.taskDoesNotExist(taskId);
            }
            return task;
        });
    }

    @Override
    public Completable storeTask(Task task) {
        return Completable.fromAction(() -> tasks.put(task.getId(), task));
    }

    @Override
    public Completable updateTask(Task task) {
        return storeTask(task);
    }

    @Override
    public Completable replaceTask(Task oldTask, Task newTask) {
        return Completable.fromAction(() -> {
            tasks.invalidate(oldTask.getId());
            archivedTasks.put(oldTask.getId(), oldTask);
            tasks.put(newTask.getId(), newTask);
        });
    }

    @Override
    public Completable moveTask(Job jobFrom, Job jobTo, Task taskAfter) {
        return Completable.fromAction(() -> {
            jobs.put(jobFrom.getId(), jobFrom);
            jobs.put(jobTo.getId(), jobTo);
            tasks.put(taskAfter.getId(), taskAfter);
        });
    }

    @Override
    public Completable deleteTask(Task task) {
        return Completable.fromAction(() -> {
            tasks.invalidate(task.getId());
            archivedTasks.put(task.getId(), task);
        });
    }

    @Override
    public Observable<Job<?>> retrieveArchivedJob(String jobId) {
        return Observable.fromCallable(() -> {
            Job job = archivedJobs.getIfPresent(jobId);
            if (job == null) {
                throw JobStoreException.jobDoesNotExist(jobId);
            }
            return job;
        });
    }

    @Override
    public Observable<Task> retrieveArchivedTasksForJob(String jobId) {
        return Observable.from(archivedTasks.asMap().values()).filter(task -> task.getJobId().equals(jobId));
    }

    @Override
    public Observable<Task> retrieveArchivedTask(String taskId) {
        return Observable.fromCallable(() -> {
            Task task = archivedTasks.getIfPresent(taskId);
            if (task == null) {
                throw JobStoreException.taskDoesNotExist(taskId);
            }
            return task;
        });
    }

    @Override
    public Observable<Long> retrieveArchivedTaskCountForJob(String jobId) {
        return Observable.from(archivedTasks.asMap().values()).filter(task -> task.getJobId().equals(jobId)).count().cast(Long.class);
    }

    @Override
    public Completable deleteArchivedTask(String jobId, String taskId) {
        return Completable.fromAction(() -> {
            archivedTasks.invalidate(taskId);
        });
    }
}
