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

package io.netflix.titus.master.endpoint;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.netflix.titus.api.model.Page;
import io.netflix.titus.api.model.Pagination;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.endpoint.common.TaskSummary;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import rx.Observable;

/**
 * {@link TitusServiceGateway} is a facade used by transport layers (REST, GRPC) to interact with Titus runtime.
 * It also provides automatic conversion between transport and runtime models.
 * <p/>
 * The primary driver for having this abstraction is to support migration process during v3 feature set implementation.
 * The implementation will be pursued in a few phases:
 * <ul>
 * <li>phase 1 - build v3 endpoint (REST, GRPC), and bridge it to old Titus runtime</li>
 * <li>phase 2 - build prototype runtime for pod support. Endpoint layers will interact with both runtime layers, depending on the client request</li>
 * <li>phase 3 - remove old runtime, and connect both v2 and v3 endpoints to the new implementation</li>
 * </ul>
 * <p>
 * {@link TitusServiceGateway} layer will be responsible for:
 * <ul>
 * <li>request dispatching to old or new runtime</li>
 * <li>endpoints data model conversions to old or new runtime</li>
 * </ul>
 * The {@link TitusServiceGateway} interface itself should be close to the future runtime API. Once phase 3 is
 * complete, most likely we will remove this abstraction, and let v2/v3 endpoints talk directly to the new runtime API.
 * <h3>Rx contract</h3>
 * Method invocations create observable results, that have to be subscribed to, to execute the operation.
 */
public interface TitusServiceGateway<USER, JOB_SPEC, JOB_TYPE extends Enum<JOB_TYPE>, JOB, TASK, TASK_STATE> {

    /*
     * Job management API
     */

    /**
     * Create a new job according to the provided specification.
     *
     * @return observable that is completed once the job is created
     */
    Observable<String> createJob(JOB_SPEC jobSpec);

    /**
     * Kill a job with the given id.
     *
     * @return observable that onComplets if the job is found, and kill request is dispatched. Error otherwise.
     */
    Observable<Void> killJob(USER user, String jobId);

    /**
     * Given job id, return its representation.
     */
    Observable<JOB> findJobById(String jobId, boolean includeArchivedTasks, Set<TASK_STATE> taskStates);

    /**
     * Return a collection of jobs matching the given query criteria.
     */
    Pair<List<JOB>, Pagination> findJobsByCriteria(JobQueryCriteria<TASK_STATE, JOB_TYPE> queryCriteria, Optional<Page> page);

    /**
     * Returns a collection of tasks matching the given query criteria.
     */
    Pair<List<TASK>, Pagination> findTasksByCriteria(JobQueryCriteria<TASK_STATE, JOB_TYPE> queryCriteria, Optional<Page> page);

    /**
     * Given task id, return its representation.
     */
    Observable<TASK> findTaskById(String taskId);

    /**
     * Change the number of parallel tasks of the given job.
     */
    Observable<Void> resizeJob(USER user, String jobId, int desired, int min, int max);

    /**
     * Update job processes
     */
    Observable<Void> updateJobProcesses(USER user, String jobId, boolean disableDecreaseDesired, boolean disableIncreaseDesired);

    /**
     * Change job 'inService' status.
     *
     * @return emits error if the job is not found or it is not a service job
     */
    Observable<Void> changeJobInServiceStatus(USER user, String serviceJobId, boolean inService);

    /**
     * Kill a given task, optionally shrinking its job by one element.
     */
    Observable<Void> killTask(String user, String taskId, boolean shrink);

    Observable<List<TaskSummary>> getTaskSummary();

    <EVENT, EVENT_STREAM extends Observable<EVENT>> EVENT_STREAM observeJobs();

    <EVENT, EVENT_STREAM extends Observable<EVENT>> EVENT_STREAM observeJob(String jobId);
}
