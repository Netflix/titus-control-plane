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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import rx.Observable;
import rx.subjects.PublishSubject;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.isBatchJob;

class StubbedJobData {

    private final TitusRuntime titusRuntime;

    private final ConcurrentMap<String, JobHolder> jobHoldersById = new ConcurrentHashMap<>();

    private final PublishSubject<JobManagerEvent<?>> observeJobsSubject = PublishSubject.create();

    StubbedJobData(TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
    }

    List<Job> getJobs() {
        return jobHoldersById.values().stream().map(JobHolder::getJob).collect(Collectors.toList());
    }

    Optional<Job<?>> findJob(String jobId) {
        return Optional.ofNullable(jobHoldersById.get(jobId)).map(JobHolder::getJob);
    }

    List<Task> getTasks() {
        return jobHoldersById.values().stream().flatMap(h -> h.getTasksById().values().stream()).collect(Collectors.toList());
    }

    List<Task> getTasks(String jobId) {
        return Optional.ofNullable(jobHoldersById.get(jobId))
                .map(h -> (List<Task>) new ArrayList<>(h.getTasksById().values())
                )
                .orElse(Collections.emptyList());
    }

    Optional<Task> findTask(String taskId) {
        return jobHoldersById.values().stream()
                .filter(h -> h.getTasksById().containsKey(taskId))
                .map(h -> h.getTasksById().get(taskId))
                .findFirst();
    }

    Optional<ContainerHealthStatus> getTaskHealthStatus(String taskId) {
        return jobHoldersById.values().stream()
                .filter(h -> h.getTasksById().containsKey(taskId))
                .map(h -> h.getTaskHealthStatus(taskId))
                .findFirst();
    }

    void addJob(Job<?> job) {
        jobHoldersById.put(job.getId(), new JobHolder(job));
        observeJobsSubject.onNext(JobUpdateEvent.newJob(job));
    }

    String createJob(JobDescriptor<?> jobDescriptor) {
        Job<?> job = Job.newBuilder()
                .withId(UUID.randomUUID().toString())
                .withJobDescriptor((JobDescriptor) jobDescriptor)
                .withStatus(JobStatus.newBuilder()
                        .withState(JobState.Accepted)
                        .withReasonCode("created")
                        .withReasonMessage("Created by StubbedJobData")
                        .build()
                )
                .build();
        addJob(job);
        return job.getId();
    }

    Job<?> changeJob(String jobId, Function<Job<?>, Job<?>> transformer) {
        return getJobHolderByJobId(jobId).changeJob(transformer);
    }

    Job moveJobToKillInitiatedState(Job job) {
        return getJobHolderByJobId(job.getId()).moveJobToKillInitiatedState();
    }

    void killJob(String jobId) {
        getJobHolderByJobId(jobId).killJob();
        jobHoldersById.remove(jobId);
    }

    Job finishJob(Job job) {
        Job removed = getJobHolderByJobId(job.getId()).finishJob();
        jobHoldersById.remove(job.getId());
        return removed;
    }

    List<Task> createDesiredTasks(Job<?> job) {
        return getJobHolderByJobId(job.getId()).createDesiredTasks();
    }

    Task changeTask(String taskId, Function<Task, Task> transformer) {
        return getJobHolderByTaskId(taskId).changeTask(taskId, transformer);
    }

    void changeContainerHealth(String taskId, ContainerHealthState healthState) {
        getJobHolderByTaskId(taskId).changeContainerHealth(taskId, healthState);
    }

    Task moveTaskToState(Task task, TaskState newState) {
        return getJobHolderByTaskId(task.getId()).moveTaskToState(task, newState);
    }

    void killTask(String taskId, boolean shrink, String reason) {
        getJobHolderByTaskId(taskId).killTask(taskId, shrink, reason);
    }

    void removeTask(Task task, boolean requireFinishedState) {
        getJobHolderByTaskId(task.getId()).removeTask(task, requireFinishedState);
    }

    public Observable<JobManagerEvent<?>> events(boolean snapshot) {
        return snapshot ? ObservableExt.fromCollection(this::getEventSnapshot).concatWith(observeJobsSubject) : observeJobsSubject;
    }

    private JobHolder getJobHolderByJobId(String jobId) {
        JobHolder jobHolder = jobHoldersById.get(jobId);
        if (jobHolder == null) {
            throw JobManagerException.jobNotFound(jobId);
        }
        return jobHolder;
    }

    private JobHolder getJobHolderByTaskId(String taskId) {
        return jobHoldersById.values().stream()
                .filter(h -> h.getTasksById().containsKey(taskId))
                .findFirst()
                .orElseThrow(() -> JobManagerException.taskNotFound(taskId));
    }

    private Collection<JobManagerEvent<?>> getEventSnapshot() {
        List<JobManagerEvent<?>> events = new ArrayList<>();

        jobHoldersById.forEach((jobId, jobHolder) -> {
            events.add(JobUpdateEvent.newJob(jobHolder.getJob()));
            jobHolder.getTasksById().forEach((taskId, task) -> events.add(TaskUpdateEvent.newTask(jobHolder.getJob(), task)));
        });

        events.add(JobManagerEvent.snapshotMarker());
        return events;
    }

    private class JobHolder {

        private Job<?> job;
        private final ConcurrentMap<String, Task> tasksById = new ConcurrentHashMap<>();
        private final Map<String, ContainerHealthStatus> tasksHealthById = new HashMap<>();
        private final MutableDataGenerator<Task> taskGenerator;

        JobHolder(Job<?> job) {
            this.job = job;
            DataGenerator immutableTaskGenerator = JobFunctions.isServiceJob(job)
                    ? JobGenerator.serviceTasks((Job<ServiceJobExt>) job)
                    : JobGenerator.batchTasks((Job<BatchJobExt>) job);
            this.taskGenerator = new MutableDataGenerator<>(immutableTaskGenerator);

        }

        Job<?> getJob() {
            return job;
        }

        Map<String, Task> getTasksById() {
            return tasksById;
        }

        ContainerHealthStatus getTaskHealthStatus(String taskId) {
            Task task = tasksById.get(taskId);
            if (task == null) {
                return ContainerHealthStatus.unknown(taskId, "not found", titusRuntime.getClock().wallTime());
            }
            if (task.getStatus().getState() != TaskState.Started) {
                return ContainerHealthStatus.unhealthy(taskId, "not started", titusRuntime.getClock().wallTime());
            }
            return tasksHealthById.computeIfAbsent(taskId, tid -> ContainerHealthStatus.healthy(taskId, titusRuntime.getClock().wallTime()));
        }

        Job<?> changeJob(Function<Job<?>, Job<?>> transformer) {
            Job<?> currentJob = job;
            this.job = transformer.apply(job);
            observeJobsSubject.onNext(JobUpdateEvent.jobChange(job, currentJob));
            return job;
        }

        Job moveJobToKillInitiatedState() {
            Preconditions.checkState(job.getStatus().getState() == JobState.Accepted);

            Job<?> currentJob = job;
            this.job = job.toBuilder()
                    .withStatus(
                            JobStatus.newBuilder()
                                    .withState(JobState.KillInitiated)
                                    .withReasonCode("killed")
                                    .withReasonMessage("call to moveJobToKillInitiatedState")
                                    .withTimestamp(titusRuntime.getClock().wallTime())
                                    .build()
                    ).build();

            observeJobsSubject.onNext(JobUpdateEvent.jobChange(job, currentJob));

            return job;
        }

        void killJob() {
            throw new IllegalStateException("not implemented yet");
        }

        Job finishJob() {
            Preconditions.checkState(job.getStatus().getState() == JobState.KillInitiated);
            Preconditions.checkState(tasksById.isEmpty());

            Job<?> currentJob = job;

            this.job = currentJob.toBuilder()
                    .withStatus(
                            JobStatus.newBuilder()
                                    .withState(JobState.Finished)
                                    .withReasonCode("finished")
                                    .withReasonMessage("call to finishJob")
                                    .withTimestamp(titusRuntime.getClock().wallTime())
                                    .build()
                    ).build();

            observeJobsSubject.onNext(JobUpdateEvent.jobChange(job, currentJob));

            return job;
        }

        List<Task> createDesiredTasks() {
            int desired = JobFunctions.getJobDesiredSize(job);
            int missing = desired - tasksById.size();

            List<Task> newTasks = taskGenerator.getValues(missing);
            newTasks.forEach(task -> {
                tasksById.put(task.getId(), task);
                observeJobsSubject.onNext(TaskUpdateEvent.newTask(job, task));
            });

            // Now replace finished tasks with new tasks
            tasksById.values().forEach(task -> {
                if (task.getStatus().getState() == TaskState.Finished) {
                    tasksById.remove(task.getId());

                    Task.TaskBuilder<?, ?> taskBuilder = taskGenerator.getValue().toBuilder()
                            .withResubmitNumber(task.getResubmitNumber() + 1)
                            .withOriginalId(task.getOriginalId())
                            .withResubmitOf(task.getId());

                    if (JobFunctions.isBatchJob(job)) {
                        BatchJobTask.Builder batchTaskBuilder = (BatchJobTask.Builder) taskBuilder;
                        batchTaskBuilder.withIndex(((BatchJobTask) task).getIndex());
                    }

                    Task newTask = taskBuilder.build();

                    tasksById.put(newTask.getId(), newTask);

                    newTasks.add(newTask);
                }
            });

            return newTasks;
        }

        Task changeTask(String taskId, Function<Task, Task> transformer) {
            Job<?> job = getJobHolderByTaskId(taskId).getJob();
            Task currentTask = tasksById.get(taskId);
            Task updatedTask = transformer.apply(currentTask);
            boolean moved = currentTask != null && !currentTask.getJobId().equals(updatedTask.getJobId());
            tasksById.put(updatedTask.getId(), updatedTask);

            TaskUpdateEvent taskUpdateEvent;
            if (moved) {
                taskUpdateEvent = TaskUpdateEvent.newTaskFromAnotherJob(job, updatedTask);
            } else if (currentTask != null) {
                taskUpdateEvent = TaskUpdateEvent.taskChange(job, updatedTask, currentTask);
            } else {
                taskUpdateEvent = TaskUpdateEvent.newTask(job, updatedTask);
            }
            observeJobsSubject.onNext(taskUpdateEvent);

            return updatedTask;
        }

        void changeContainerHealth(String taskId, ContainerHealthState healthState) {
            tasksHealthById.put(taskId, ContainerHealthStatus.newBuilder()
                    .withTaskId(taskId)
                    .withState(healthState)
                    .withReason("On demand change")
                    .withTimestamp(titusRuntime.getClock().wallTime())
                    .build()
            );
        }

        Task moveTaskToState(Task task, TaskState newState) {
            Task currentTask = tasksById.get(task.getId());
            Preconditions.checkState(currentTask != null && TaskState.isBefore(currentTask.getStatus().getState(), newState));

            Task updatedTask = task.toBuilder()
                    .withStatus(
                            TaskStatus.newBuilder()
                                    .withState(newState)
                                    .withReasonCode("test")
                                    .withReasonMessage("call to moveTaskToState")
                                    .withTimestamp(titusRuntime.getClock().wallTime())
                                    .build()
                    ).build();

            tasksById.put(task.getId(), updatedTask);
            observeJobsSubject.onNext(TaskUpdateEvent.taskChange(job, updatedTask, currentTask));

            return updatedTask;
        }

        void killTask(String taskId, boolean shrink, String reason) {
            Task currentTask = tasksById.get(taskId);
            TaskState taskState = currentTask.getStatus().getState();
            switch (taskState) {
                case Accepted:
                case Disconnected:
                case KillInitiated:
                    moveTaskToState(currentTask, TaskState.Finished);
                    break;
                case Launched:
                case StartInitiated:
                case Started:
                    moveTaskToState(currentTask, TaskState.KillInitiated);
                    moveTaskToState(currentTask, TaskState.Finished);
                    break;
                case Finished:
                    break;
            }
            observeJobsSubject.onNext(TaskUpdateEvent.taskChange(job, tasksById.get(taskId), currentTask));

            if (shrink) {
                tasksById.remove(taskId);
                changeJob(job -> {
                    Job<?> updatedJob;
                    int desired = JobFunctions.getJobDesiredSize(job);
                    if (JobFunctions.isServiceJob(job)) {
                        Capacity capacity = ((ServiceJobExt) job.getJobDescriptor().getExtensions()).getCapacity();
                        Capacity newCapacity = capacity.toBuilder().withDesired(desired).build();
                        updatedJob = JobFunctions.changeServiceJobCapacity((Job) job, newCapacity);
                    } else {
                        updatedJob = JobFunctions.changeBatchJobSize((Job) job, desired);
                    }
                    return updatedJob;
                });
            } else {
                Task newTask = taskGenerator.getValue();
                if (isBatchJob(job)) {
                    String index = currentTask.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX);
                    Map<String, String> newContext = CollectionsExt.copyAndAdd(currentTask.getTaskContext(), TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX, index);
                    newTask = newTask.toBuilder().withTaskContext(newContext).build();
                }
                tasksById.put(newTask.getId(), newTask);
                observeJobsSubject.onNext(TaskUpdateEvent.newTask(job, newTask));
            }
        }

        void removeTask(Task task, boolean requireFinishedState) {
            Task currentTask = tasksById.get(task.getId());
            if (requireFinishedState) {
                Preconditions.checkState(currentTask != null && currentTask.getStatus().getState() == TaskState.Finished);
            }
            tasksById.remove(task.getId());
        }
    }
}
