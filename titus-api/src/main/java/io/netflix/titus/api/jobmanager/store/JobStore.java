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

package io.netflix.titus.api.jobmanager.store;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;
import rx.Completable;
import rx.Observable;

/**
 * Provides an abstraction to the underlying datastore.
 */
public interface JobStore {

    /**
     * Initialize the store.
     */
    Completable init();

    /**
     * Retrieve all jobs.
     *
     * @return all the jobs.
     */
    Observable<Job<?>> retrieveJobs();

    /**
     * Retrieve the job with the specified jobId.
     *
     * @param jobId
     * @return the job or an error if it does not exist.
     */
    Observable<Job<?>> retrieveJob(String jobId);

    /**
     * Store a new job.
     *
     * @param job
     */
    Completable storeJob(Job job);

    /**
     * Update an existing job.
     *
     * @param job
     */
    Completable updateJob(Job job);

    /**
     * Delete a job
     *
     * @param job
     */
    Completable deleteJob(Job job);

    /**
     * Retrieve all the tasks for a specific job.
     *
     * @param jobId
     * @return the tasks for the job.
     */
    Observable<Task> retrieveTasksForJob(String jobId);

    /**
     * Retrieve a specific task.
     *
     * @param taskId
     * @return the task or an error if it is not found.
     */
    Observable<Task> retrieveTask(String taskId);

    /**
     * Store a new task.
     *
     * @param task
     */
    Completable storeTask(Task task);

    /**
     * Update an existing task.
     *
     * @param task
     */
    Completable updateTask(Task task);

    /**
     * Replace an existing task.
     *
     * @param oldTask
     * @param newTask
     */
    Completable replaceTask(Task oldTask, Task newTask);

    /**
     * Delete an existing task.
     *
     * @param task
     */
    Completable deleteTask(Task task);

    /**
     * Retrieve the archived job with the specified jobId.
     *
     * @param jobId
     * @return the job or an error if it does not exist.
     */
    Observable<Job<?>> retrieveArchivedJob(String jobId);

    /**
     * Retrieve all the archived tasks for a specific job.
     *
     * @param jobId
     * @return the archived tasks for the job.
     */
    Observable<Task> retrieveArchivedTasksForJob(String jobId);

    /**
     * Retrieve a specific archived task.
     *
     * @param taskId
     * @return the task or an error if it is not found.
     */
    Observable<Task> retrieveArchivedTask(String taskId);
}
