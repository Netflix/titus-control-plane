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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.ExecutableStatus;
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
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import rx.Observable;
import rx.subjects.PublishSubject;

public class JobGeneratorOrchestrator {

    private static final JobChangeNotification GRPC_SNAPSHOT_MARKER = JobChangeNotification.newBuilder().setSnapshotEnd(JobChangeNotification.SnapshotEnd.getDefaultInstance()).build();

    private final TitusRuntime titusRuntime;

    private final MutableDataGenerator<Job> jobGenerator;
    private final Map<String, MutableDataGenerator<JobDescriptor>> jobTemplates = new HashMap<>();

    private final Map<String, Pair<Job, MutableDataGenerator<Task>>> jobsById = new HashMap<>();
    private final Map<String, Map<String, Task>> tasksByJobId = new HashMap<>();

    private final PublishSubject<JobManagerEvent> observeJobsSubject = PublishSubject.create();

    private final ReadOnlyJobOperations readOnlyJobOperations = new ReadOnlyJobOperationsStub();

    public JobGeneratorOrchestrator(TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.jobGenerator = new MutableDataGenerator<>(JobGenerator.jobs(titusRuntime.getClock()));
    }

    public ReadOnlyJobOperations getReadOnlyJobOperations() {
        return readOnlyJobOperations;
    }

    public JobGeneratorOrchestrator addJobTemplate(String templateId, DataGenerator<JobDescriptor> jobDescriptorGenerator) {
        jobTemplates.put(templateId, new MutableDataGenerator<>(jobDescriptorGenerator));
        return this;
    }

    public JobGeneratorOrchestrator addBatchTemplate(String templateId, DataGenerator<JobDescriptor<BatchJobExt>> jobDescriptorGenerator) {
        return addJobTemplate(templateId, (DataGenerator) jobDescriptorGenerator);
    }

    public Job createJob(String templateId) {
        Job job = jobGenerator.getValue().toBuilder().withJobDescriptor(jobTemplates.get(templateId).getValue()).build();

        DataGenerator taskGenerator = JobFunctions.isServiceJob(job)
                ? JobGenerator.serviceTasks(job)
                : JobGenerator.batchTasks(job);
        jobsById.put(job.getId(), Pair.of(job, new MutableDataGenerator<>(taskGenerator)));

        observeJobsSubject.onNext(JobUpdateEvent.newJob(job));

        return job;
    }

    public List<Task> createDesiredTasks(Job<?> job) {
        Pair<Job, MutableDataGenerator<Task>> current = jobsById.get(job.getId());
        Job currentJob = current.getLeft();
        MutableDataGenerator<Task> taskGenerator = current.getRight();

        Map<String, Task> currentTasks = tasksByJobId.computeIfAbsent(job.getId(), j -> new HashMap<>());
        int desired = JobFunctions.isServiceJob(currentJob)
                ? ((ServiceJobExt) currentJob.getJobDescriptor().getExtensions()).getCapacity().getDesired()
                : ((BatchJobExt) currentJob.getJobDescriptor().getExtensions()).getSize();


        int missing = desired - currentTasks.size();
        List<Task> newTasks = taskGenerator.getValues(missing);
        newTasks.forEach(task -> {
            currentTasks.put(task.getId(), task);
            observeJobsSubject.onNext(TaskUpdateEvent.newTask(currentJob, task));
        });

        return newTasks;
    }

    public Pair<Job, List<Task>> createJobAndTasks(String templateId) {
        Job job = createJob(templateId);
        List<Task> tasks = createDesiredTasks(job);
        return Pair.of(job, tasks);
    }

    public Job createJobAndTasks(String templateId, BiConsumer<Job, List<Task>> processor) {
        Pair<Job, List<Task>> pair = createJobAndTasks(templateId);
        processor.accept(pair.getLeft(), pair.getRight());
        return pair.getLeft();
    }

    public List<Pair<Job, List<Task>>> creteMultipleJobsAndTasks(String... templateIds) {
        List<Pair<Job, List<Task>>> result = new ArrayList<>();
        for (String templateId : templateIds) {
            result.add(createJobAndTasks(templateId));
        }
        return result;
    }

    public Job moveJobToKillInitiatedState(Job job) {
        Preconditions.checkState(jobsById.containsKey(job.getId()));
        Pair<Job, MutableDataGenerator<Task>> current = jobsById.get(job.getId());

        Job currentJob = current.getLeft();
        Preconditions.checkState(currentJob.getStatus().getState() == JobState.Accepted);

        Job updatedJob = currentJob.toBuilder()
                .withStatus(
                        JobStatus.newBuilder()
                                .withState(JobState.KillInitiated)
                                .withReasonCode("killed")
                                .withReasonMessage("call to moveJobToKillInitiatedState")
                                .build()
                ).build();

        jobsById.put(job.getId(), Pair.of(updatedJob, current.getRight()));

        observeJobsSubject.onNext(JobUpdateEvent.jobChange(updatedJob, currentJob));

        return updatedJob;
    }

    public Job finishJob(Job job) {
        Preconditions.checkState(jobsById.containsKey(job.getId()));
        Preconditions.checkState(tasksByJobId.getOrDefault(job.getId(), Collections.emptyMap()).size() == 0);

        Pair<Job, MutableDataGenerator<Task>> current = jobsById.remove(job.getId());
        Job currentJob = current.getLeft();
        Preconditions.checkState(currentJob.getStatus().getState() == JobState.KillInitiated);

        Job updatedJob = currentJob.toBuilder()
                .withStatus(
                        JobStatus.newBuilder()
                                .withState(JobState.Finished)
                                .withReasonCode("finished")
                                .withReasonMessage("call to finishJob")
                                .build()
                ).build();

        observeJobsSubject.onNext(JobUpdateEvent.jobChange(updatedJob, currentJob));

        return updatedJob;
    }

    public Task moveTaskToState(String taskId, TaskState newState) {
        Pair<Job<?>, Task> jobTaskPair = readOnlyJobOperations.findTaskById(taskId).orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskId));
        return moveTaskToState(jobTaskPair.getRight(), newState);
    }

    public Task moveTaskToState(Task task, TaskState newState) {
        Preconditions.checkState(jobsById.containsKey(task.getJobId()));

        Job job = jobsById.get(task.getJobId()).getLeft();
        Map<String, Task> tasks = tasksByJobId.get(task.getJobId());

        Task currentTask = tasks.get(task.getId());
        Preconditions.checkState(currentTask != null && TaskState.isBefore(currentTask.getStatus().getState(), newState));

        Task updatedTask = task.toBuilder()
                .withStatus(
                        TaskStatus.newBuilder()
                                .withState(newState)
                                .withReasonCode("test")
                                .withReasonMessage("call to moveTaskToState")
                                .build()
                ).build();

        tasksByJobId.put(task.getJobId(), CollectionsExt.copyAndAdd(tasks, task.getId(), updatedTask));

        observeJobsSubject.onNext(TaskUpdateEvent.taskChange(job, updatedTask, currentTask));

        return updatedTask;
    }

    public void forget(Task task) {
        internalTaskRemove(task, false);
    }

    public void removeTask(Task task) {
        internalTaskRemove(task, true);
    }

    private void internalTaskRemove(Task task, boolean requireFinishedState) {
        Preconditions.checkState(jobsById.containsKey(task.getJobId()));

        Job job = jobsById.get(task.getJobId()).getLeft();
        Map<String, Task> tasks = tasksByJobId.get(task.getJobId());

        Task currentTask = tasks.get(task.getId());
        if (requireFinishedState) {
            Preconditions.checkState(currentTask != null && currentTask.getStatus().getState() == TaskState.Finished);
        }

        tasksByJobId.put(job.getId(), CollectionsExt.copyAndRemove(tasks, task.getId()));
    }

    public List<Pair<Job, List<Task>>> getJobsAndTasks() {
        return jobsById.values().stream()
                .map(Pair::getLeft)
                .map(job -> Pair.of(job, (List<Task>) new ArrayList<>(tasksByJobId.get(job.getId()).values())))
                .collect(Collectors.toList());
    }

    public Observable<JobManagerEvent> observeJobs(boolean snapshot) {
        return snapshot ? ObservableExt.fromCollection(this::getEventSnapshot).concatWith(observeJobsSubject) : observeJobsSubject;
    }

    public Observable<JobChangeNotification> grpcObserveJobs(boolean snapshot) {
        Observable<JobChangeNotification> result = observeJobsSubject.map(event -> V3GrpcModelConverters.toGrpcJobChangeNotification(event, EmptyLogStorageInfo.empty()));
        if (!snapshot) {
            return result;
        }

        return ObservableExt.fromCollection(this::getEventSnapshot)
                .map(event -> V3GrpcModelConverters.toGrpcJobChangeNotification(event, EmptyLogStorageInfo.empty()))
                .concatWith(Observable.just(GRPC_SNAPSHOT_MARKER))
                .concatWith(result);
    }

    private Collection<JobManagerEvent> getEventSnapshot() {
        List<JobManagerEvent> events = new ArrayList<>();
        jobsById.values().forEach(pair -> events.add(JobUpdateEvent.newJob(pair.getLeft())));
        tasksByJobId.values().forEach(tasksById -> tasksById.values().forEach(task -> events.add(TaskUpdateEvent.newTask(jobsById.get(task.getJobId()).getLeft(), task))));
        return events;
    }

    private class ReadOnlyJobOperationsStub implements ReadOnlyJobOperations {

        @Override
        public List<Job> getJobs() {
            return jobsById.values().stream().map(Pair::getLeft).collect(Collectors.toList());
        }

        @Override
        public Optional<Job<?>> getJob(String jobId) {
            return Optional.ofNullable(jobsById.get(jobId)).map(Pair::getLeft);
        }

        @Override
        public List<Task> getTasks() {
            return tasksByJobId.values().stream().flatMap(p -> p.values().stream()).collect(Collectors.toList());
        }

        @Override
        public List<Task> getTasks(String jobId) {
            return new ArrayList<>(tasksByJobId.getOrDefault(jobId, Collections.emptyMap()).values());
        }

        @Override
        public List<Pair<Job, List<Task>>> getJobsAndTasks() {
            return getJobs().stream().map(j -> Pair.of(j, getTasks(j.getId()))).collect(Collectors.toList());
        }

        @Override
        public List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit) {
            List<Job> allSortedJobs = getJobs();
            allSortedJobs.sort(Comparator.comparingLong(this::getJobAcceptedTimeStamp));

            List filtered = allSortedJobs.stream()
                    .filter(j -> queryPredicate.test(Pair.of((Job<?>) j, getTasks(j.getId()))))
                    .collect(Collectors.toList());
            if (filtered.isEmpty() || filtered.size() <= offset) {
                return Collections.emptyList();
            }
            return filtered.subList(offset, Math.min(filtered.size(), offset + limit));
        }

        @Override
        public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
            List<Task> allSortedTasks = getTasks();
            allSortedTasks.sort(Comparator.comparingLong(this::getTaskAcceptedTimesStamp));

            List<Pair<Job<?>, Task>> filtered = allSortedTasks.stream()
                    .map(t -> Pair.<Job<?>, Task>of(getJob(t.getJobId()).get(), t))
                    .filter(queryPredicate)
                    .collect(Collectors.toList());

            if (filtered.isEmpty() || filtered.size() <= offset) {
                return Collections.emptyList();
            }
            return filtered.subList(offset, Math.min(filtered.size(), offset + limit));
        }

        @Override
        public Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
            return getTasks().stream().filter(t -> t.getId().equals(taskId)).findFirst().map(t -> Pair.of(getJob(t.getJobId()).get(), t));
        }

        @Override
        public Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate, Predicate<Pair<Job<?>, Task>> tasksPredicate) {
            return (Observable) JobGeneratorOrchestrator.this.observeJobs(false).filter(event -> {
                if (event instanceof JobUpdateEvent) {
                    Job<?> job = ((JobUpdateEvent) event).getCurrent();
                    return jobsPredicate.test(Pair.of(job, getTasks(job.getId())));
                }
                return tasksPredicate.test(Pair.of(((TaskUpdateEvent) event).getCurrentJob(), ((TaskUpdateEvent) event).getCurrentTask()));
            });
        }

        @Override
        public Observable<JobManagerEvent<?>> observeJob(String jobId) {
            return (Observable) JobGeneratorOrchestrator.this.observeJobs(false).filter(event -> isJobEventOf(event, jobId));
        }

        private boolean isJobEventOf(JobManagerEvent event, String jobId) {
            if (event instanceof JobUpdateEvent) {
                return jobId.equals(((JobUpdateEvent) event).getCurrent().getId());
            }
            return jobId.equals(((TaskUpdateEvent) event).getCurrentJob().getId());
        }

        private long getJobAcceptedTimeStamp(Job job) {
            return JobFunctions.findJobStatus(job, JobState.Accepted).map(ExecutableStatus::getTimestamp).orElse(job.getStatus().getTimestamp());
        }

        private long getTaskAcceptedTimesStamp(Task task) {
            return JobFunctions.findTaskStatus(task, TaskState.Accepted).map(ExecutableStatus::getTimestamp).orElse(task.getStatus().getTimestamp());
        }
    }
}
