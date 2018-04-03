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

package com.netflix.titus.master.endpoint.adapter;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.master.endpoint.common.TaskSummary;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import rx.Observable;

/**
 * {@link TitusServiceGateway} adapter class that provides a single 'adapt' method for plugging-in custom logic
 * around the client request.
 */
public abstract class TitusServiceGatewayAdapter<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE> implements
        TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> {

    private final TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> delegate;

    protected TitusServiceGatewayAdapter(TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE, JOB, TASK, TASK_STATE> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Observable<String> createJob(JOB_SPEC job_spec) {
        return adapt(delegate.createJob(job_spec));
    }

    @Override
    public Observable<Void> killJob(USER user, String jobId) {
        return adapt(delegate.killJob(user, jobId));
    }

    @Override
    public Observable<JOB> findJobById(String jobId, boolean includeArchivedTasks, Set<TASK_STATE> taskStates) {
        return adapt(delegate.findJobById(jobId, includeArchivedTasks, taskStates));
    }

    @Override
    public Pair<List<JOB>, Pagination> findJobsByCriteria(JobQueryCriteria<TASK_STATE, JOB_TYPE> queryCriteria, Optional<Page> page) {
        return adapt(() -> delegate.findJobsByCriteria(queryCriteria, page));
    }

    @Override
    public Pair<List<TASK>, Pagination> findTasksByCriteria(JobQueryCriteria<TASK_STATE, JOB_TYPE> queryCriteria, Optional<Page> page) {
        return adapt(() -> delegate.findTasksByCriteria(queryCriteria, page));
    }

    @Override
    public Observable<TASK> findTaskById(String taskId) {
        return adapt(delegate.findTaskById(taskId));
    }

    @Override
    public Observable<Void> resizeJob(USER user, String jobId, int desired, int min, int max) {
        return adapt(delegate.resizeJob(user, jobId, desired, min, max));
    }

    @Override
    public Observable<Void> updateJobProcesses(USER user, String jobId, boolean disableDecreaseDesired, boolean disableIncreaseDesired) {
        return adapt(delegate.updateJobProcesses(user, jobId, disableDecreaseDesired, disableIncreaseDesired));
    }

    @Override
    public Observable<Void> changeJobInServiceStatus(USER user, String serviceJobId, boolean inService) {
        return adapt(delegate.changeJobInServiceStatus(user, serviceJobId, inService));
    }

    @Override
    public <EVENT, EVENT_STREAM extends Observable<EVENT>> EVENT_STREAM observeJobs() {
        return delegate.observeJobs();
    }

    @Override
    public <EVENT, EVENT_STREAM extends Observable<EVENT>> EVENT_STREAM observeJob(String jobId) {
        return delegate.observeJob(jobId);
    }

    @Override
    public Observable<Void> killTask(String user, String taskId, boolean shrink) {
        return adapt(delegate.killTask(user, taskId, shrink));
    }

    protected abstract <T> Observable<T> adapt(Observable<T> replyStream);

    protected abstract <T> T adapt(Supplier<T> replySupplier);

    @Override
    public Observable<List<TaskSummary>> getTaskSummary() {
        return adapt(delegate.getTaskSummary());
    }
}
