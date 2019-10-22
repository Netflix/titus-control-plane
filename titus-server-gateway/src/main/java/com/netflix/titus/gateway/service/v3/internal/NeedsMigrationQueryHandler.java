/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.gateway.service.v3.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.PageResult;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.relocation.RelocationDataReplicator;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.api.jobmanager.model.job.LogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.runtime.endpoint.v3.grpc.query.V3TaskQueryCriteriaEvaluator;
import com.netflix.titus.runtime.jobmanager.JobManagerCursors;

import static com.netflix.titus.gateway.service.v3.internal.TaskRelocationDataInjector.newTaskWithRelocationPlan;

@Singleton
class NeedsMigrationQueryHandler {

    private final JobDataReplicator jobDataReplicator;
    private final RelocationDataReplicator relocationDataReplicator;
    private final LogStorageInfo<Task> logStorageInfo;
    private final TitusRuntime titusRuntime;

    @Inject
    NeedsMigrationQueryHandler(JobDataReplicator jobDataReplicator,
                               RelocationDataReplicator relocationDataReplicator,
                               LogStorageInfo<Task> logStorageInfo,
                               TitusRuntime titusRuntime) {
        this.jobDataReplicator = jobDataReplicator;
        this.relocationDataReplicator = relocationDataReplicator;
        this.logStorageInfo = logStorageInfo;
        this.titusRuntime = titusRuntime;
    }

    PageResult<com.netflix.titus.grpc.protogen.Task> findTasks(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria, Page page) {
        List<Pair<Job<?>, List<Task>>> jobsAndTasks = jobDataReplicator.getCurrent().getJobsAndTasks();
        Map<String, TaskRelocationPlan> relocationPlans = relocationDataReplicator.getCurrent().getPlans();

        V3TaskQueryCriteriaEvaluator queryFilter = new V3TaskQueryCriteriaEvaluator(queryCriteria, titusRuntime);
        V3TaskQueryCriteriaEvaluator queryFilterWithoutNeedsMigration = new V3TaskQueryCriteriaEvaluator(filterOutNeedsMigration(queryCriteria), titusRuntime);

        List<com.netflix.titus.grpc.protogen.Task> matchingTasks = new ArrayList<>();
        jobsAndTasks.forEach(jobTasksPair -> {
            Job<?> job = jobTasksPair.getLeft();
            List<Task> tasks = jobTasksPair.getRight();
            tasks.forEach(task -> {

                TaskRelocationPlan plan = relocationPlans.get(task.getId());

                Pair<Job<?>, Task> jobTaskPair = Pair.of(job, task);
                if (plan != null) {
                    if (queryFilterWithoutNeedsMigration.test(jobTaskPair)) {
                        matchingTasks.add(newTaskWithRelocationPlan(GrpcJobManagementModelConverters.toGrpcTask(task, logStorageInfo), plan));
                    }
                } else {
                    if (queryFilter.test(jobTaskPair)) {
                        matchingTasks.add(GrpcJobManagementModelConverters.toGrpcTask(task, logStorageInfo));
                    }
                }
            });
        });

        Pair<List<com.netflix.titus.grpc.protogen.Task>, Pagination> paginationPair = PaginationUtil.takePageWithCursor(
                page,
                matchingTasks,
                JobManagerCursors.taskCursorOrderComparator(),
                JobManagerCursors::taskIndexOf,
                JobManagerCursors::newCursorFrom
        );

        return PageResult.pageOf(paginationPair.getLeft(), paginationPair.getRight());
    }

    private JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> filterOutNeedsMigration(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> queryCriteria) {
        return queryCriteria.toBuilder().withNeedsMigration(false).build();
    }
}
