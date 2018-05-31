package com.netflix.titus.testkit.model.job;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
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

    private static final JobChangeNotification.Builder GRPC_SNAPSHOT_MARKER = JobChangeNotification.newBuilder().setSnapshotEnd(JobChangeNotification.SnapshotEnd.getDefaultInstance());

    private final TitusRuntime titusRuntime;

    private final MutableDataGenerator<Job> jobGenerator;
    private final Map<String, MutableDataGenerator<JobDescriptor>> jobTemplates = new HashMap<>();

    private final Map<String, Pair<Job, MutableDataGenerator<Task>>> jobsById = new HashMap<>();
    private final Map<String, Map<String, Task>> tasksByJobId = new HashMap<>();

    private final PublishSubject<JobManagerEvent> observeJobsSubject = PublishSubject.create();

    public JobGeneratorOrchestrator(TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.jobGenerator = new MutableDataGenerator<>(JobGenerator.jobs(titusRuntime.getClock()));
    }

    public void addJobTemplate(String templateId, DataGenerator<JobDescriptor> jobDescriptorGenerator) {
        jobTemplates.put(templateId, new MutableDataGenerator<>(jobDescriptorGenerator));
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

    public void removeTask(Task task) {
        Preconditions.checkState(jobsById.containsKey(task.getJobId()));

        Job job = jobsById.get(task.getJobId()).getLeft();
        Map<String, Task> tasks = tasksByJobId.get(task.getJobId());

        Task currentTask = tasks.get(task.getId());
        Preconditions.checkState(currentTask != null && currentTask.getStatus().getState() == TaskState.Finished);

        tasksByJobId.put(job.getId(), CollectionsExt.copyAndRemove(tasks, task.getId()));
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
                .concatWith(Observable.just(GRPC_SNAPSHOT_MARKER.build()))
                .concatWith(result);
    }

    private Collection<JobManagerEvent> getEventSnapshot() {
        List<JobManagerEvent> events = new ArrayList<>();
        jobsById.values().forEach(pair -> events.add(JobUpdateEvent.newJob(pair.getLeft())));
        tasksByJobId.values().forEach(tasksById -> tasksById.values().forEach(task -> events.add(TaskUpdateEvent.newTask(jobsById.get(task.getJobId()).getLeft(), task))));
        return events;
    }
}
