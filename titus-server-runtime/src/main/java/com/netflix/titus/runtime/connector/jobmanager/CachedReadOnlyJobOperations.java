package com.netflix.titus.runtime.connector.jobmanager;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.util.tuple.Pair;
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
        return replicator.getCurrent().getTasks(jobId);
    }

    @Override
    public List<Pair<Job, List<Task>>> getJobsAndTasks() {
        return (List) replicator.getCurrent().getJobsAndTasks();
    }

    @Override
    public List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit) {
        JobSnapshot snapshot = replicator.getCurrent();

        return snapshot.getJobs().stream()
                .filter(job -> queryPredicate.test(Pair.of(job, snapshot.getTasks(job.getId()))))
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
        JobSnapshot snapshot = replicator.getCurrent();

        return snapshot.getJobs().stream()
                .flatMap(job -> snapshot.getTasks(job.getId()).stream()
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

    @Override
    public Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate, Predicate<Pair<Job<?>, Task>> tasksPredicate) {
        throw new IllegalStateException("method not implemented yet");
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJob(String jobId) {
        throw new IllegalStateException("method not implemented yet");
    }
}
