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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.CapacityAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.event.JobKeepAliveEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.asServiceJob;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobProcesses;

class StubbedJobOperations implements V3JobOperations {

    private final StubbedJobData stubbedJobData;

    StubbedJobOperations(StubbedJobData stubbedJobData) {
        this.stubbedJobData = stubbedJobData;
    }

    @Override
    public List<Job> getJobs() {
        return stubbedJobData.getJobs();
    }

    @Override
    public Optional<Job<?>> getJob(String jobId) {
        return stubbedJobData.findJob(jobId);
    }

    @Override
    public List<Task> getTasks() {
        return stubbedJobData.getTasks();
    }

    @Override
    public List<Task> getTasks(String jobId) {
        return stubbedJobData.getTasks(jobId);
    }

    @Override
    public List<Pair<Job, List<Task>>> getJobsAndTasks() {
        return getJobs().stream().map(job -> Pair.of(job, getTasks(job.getId()))).collect(Collectors.toList());
    }

    @Override
    public List<Job<?>> findJobs(Predicate<Pair<Job<?>, List<Task>>> queryPredicate, int offset, int limit) {
        List<Pair<Job<?>, List<Task>>> jobsAndTasks = (List) getJobsAndTasks();
        return jobsAndTasks.stream()
                .filter(queryPredicate)
                .skip(offset)
                .limit(limit)
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }

    @Override
    public List<Pair<Job<?>, Task>> findTasks(Predicate<Pair<Job<?>, Task>> queryPredicate, int offset, int limit) {
        List<Pair<Job<?>, List<Task>>> jobsAndTasks = (List) getJobsAndTasks();
        return (List) jobsAndTasks.stream()
                .flatMap(p -> p.getRight().stream().map(t -> (Pair) Pair.of(p.getLeft(), t)))
                .filter(queryPredicate::test)
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Pair<Job<?>, Task>> findTaskById(String taskId) {
        return stubbedJobData
                .findTask(taskId)
                .flatMap(task -> stubbedJobData.findJob(task.getJobId()).map(j -> Pair.of(j, task)));
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJobs(Predicate<Pair<Job<?>, List<Task>>> jobsPredicate,
                                                      Predicate<Pair<Job<?>, Task>> tasksPredicate,
                                                      boolean withCheckpoints) {
        return stubbedJobData.events(false)
                .filter(event -> {
                    if (event instanceof JobKeepAliveEvent) {
                        return withCheckpoints;
                    }
                    if (event instanceof JobUpdateEvent) {
                        Job job = ((JobUpdateEvent) event).getCurrent();
                        return jobsPredicate.test(Pair.of(job, getTasks(job.getId())));
                    }
                    if (event instanceof TaskUpdateEvent) {
                        Task task = ((TaskUpdateEvent) event).getCurrentTask();
                        return stubbedJobData.findJob(task.getJobId()).map(job -> tasksPredicate.test(Pair.of(job, task))).orElse(false);
                    }
                    return false;
                });
    }

    @Override
    public Observable<JobManagerEvent<?>> observeJob(String jobId) {
        return stubbedJobData.events(false)
                .filter(event -> {
                    if (event instanceof JobUpdateEvent) {
                        Job job = ((JobUpdateEvent) event).getCurrent();
                        return job.getId().equals(jobId);
                    }
                    if (event instanceof TaskUpdateEvent) {
                        Task task = ((TaskUpdateEvent) event).getCurrentTask();
                        return task.getJobId().equals(jobId);
                    }
                    return false;
                });
    }

    @Override
    public Observable<String> createJob(JobDescriptor<?> jobDescriptor, CallMetadata callMetadata) {
        return defer(() -> stubbedJobData.createJob(jobDescriptor));
    }

    @Override
    public Observable<Void> updateJobCapacityAttributes(String jobId, CapacityAttributes capacityAttributes, CallMetadata callMetadata) {
        return updateServiceJob(jobId, job -> changeServiceJobCapacity(job, capacityAttributes));
    }

    @Override
    public Observable<Void> updateServiceJobProcesses(String jobId, ServiceJobProcesses serviceJobProcesses, CallMetadata callMetadata) {
        return updateServiceJob(jobId, job -> changeServiceJobProcesses(job, serviceJobProcesses));
    }

    @Override
    public Observable<Void> updateJobStatus(String serviceJobId, boolean enabled, CallMetadata callMetadata) {
        return updateServiceJob(serviceJobId, job -> JobFunctions.changeJobEnabledStatus(job, enabled));
    }

    @Override
    public Mono<Void> updateJobDisruptionBudget(String jobId, DisruptionBudget disruptionBudget, CallMetadata callMetadata) {
        Observable<Void> observableAction = defer(() -> {
            stubbedJobData.changeJob(jobId, job -> JobFunctions.changeDisruptionBudget(job, disruptionBudget));
        });
        return ReactorExt.toMono(observableAction);
    }

    @Override
    public Mono<Void> updateJobAttributes(String jobId, Map<String, String> attributes, CallMetadata callMetadata) {
        Observable<Void> observableAction = defer(() -> {
            stubbedJobData.changeJob(jobId, job -> JobFunctions.updateJobAttributes(job, attributes));
        });
        return ReactorExt.toMono(observableAction);
    }

    @Override
    public Mono<Void> deleteJobAttributes(String jobId, Set<String> keys, CallMetadata callMetadata) {
        Observable<Void> observableAction = defer(() -> {
            stubbedJobData.changeJob(jobId, job -> JobFunctions.deleteJobAttributes(job, keys));
        });
        return ReactorExt.toMono(observableAction);
    }

    private Observable<Void> updateServiceJob(String jobId, Function<Job<ServiceJobExt>, Job<ServiceJobExt>> transformer) {
        return defer(() -> {
            stubbedJobData.changeJob(jobId, job -> transformer.apply(asServiceJob(job)));
        });
    }

    @Override
    public Observable<Void> killJob(String jobId, String reason, CallMetadata callMetadata) {
        return defer(() -> stubbedJobData.killJob(jobId));
    }

    @Override
    public Mono<Void> killTask(String taskId, boolean shrink, boolean preventMinSizeUpdate, Trigger trigger, CallMetadata callMetadata) {
        return ReactorExt.toMono(defer(() -> stubbedJobData.killTask(taskId, shrink, preventMinSizeUpdate, trigger)));
    }

    @Override
    public Observable<Void> moveServiceTask(String sourceJobId, String targetJobId, String taskId, CallMetadata callMetadata) {
        return defer(() -> {
            stubbedJobData.changeJob(sourceJobId, job -> JobFunctions.incrementJobSize(job, -1));
            stubbedJobData.changeJob(targetJobId, job -> JobFunctions.incrementJobSize(job, 1));
            stubbedJobData.changeTask(taskId, task -> JobFunctions.moveTask(sourceJobId, targetJobId, task));
        });
    }

    @Override
    public Completable updateTask(String taskId, Function<Task, Optional<Task>> changeFunction, Trigger trigger, String reason, CallMetadata callMetadata) {
        return deferCompletable(() -> stubbedJobData.changeTask(taskId, task -> changeFunction.apply(task).orElse(task)));
    }

    private <T> Observable<T> defer(Supplier<T> action) {
        return Observable.defer(() -> Observable.just(action.get()));
    }

    private Observable<Void> defer(Runnable action) {
        return Observable.defer(() -> {
            action.run();
            return Observable.empty();
        });
    }

    private Completable deferCompletable(Runnable action) {
        return Completable.defer(() -> {
            action.run();
            return Completable.complete();
        });
    }
}
