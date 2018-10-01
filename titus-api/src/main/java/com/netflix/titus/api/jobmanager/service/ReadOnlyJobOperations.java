package com.netflix.titus.api.jobmanager.service;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.common.util.tuple.Pair;
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
        return observeJobs(alwaysTrue(), alwaysTrue());
    }

    Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                               Predicate<Pair<Job<?>, Task>> tasksPredicate);

    Observable<JobManagerEvent<?>> observeJob(String jobId);
}
