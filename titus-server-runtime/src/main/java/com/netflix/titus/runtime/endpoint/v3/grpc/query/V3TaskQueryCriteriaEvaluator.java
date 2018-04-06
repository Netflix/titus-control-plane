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

package com.netflix.titus.runtime.endpoint.v3.grpc.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;

public class V3TaskQueryCriteriaEvaluator extends V3AbstractQueryCriteriaEvaluator<Task> {

    public V3TaskQueryCriteriaEvaluator(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria, TitusRuntime titusRuntime) {
        super(createTaskPredicates(criteria, titusRuntime), criteria);
    }

    private static List<Predicate<Pair<Job<?>, Task>>> createTaskPredicates(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria, TitusRuntime titusRuntime) {
        List<Predicate<Pair<Job<?>, Task>>> predicates = new ArrayList<>();
        applyJobIds(criteria.getJobIds()).ifPresent(predicates::add);
        applyTaskIds(criteria.getTaskIds()).ifPresent(predicates::add);
        applyTaskStates(criteria.getTaskStates()).ifPresent(predicates::add);
        applyTaskStateReasons(criteria.getTaskStateReasons()).ifPresent(predicates::add);
        applyNeedsMigration(criteria.isNeedsMigration(), titusRuntime).ifPresent(predicates::add);
        return predicates;
    }

    private static Optional<Predicate<Pair<Job<?>, Task>>> applyJobIds(Set<String> jobIds) {
        if (jobIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jobAndTask -> jobIds.contains(jobAndTask.getLeft().getId()));
    }

    private static Optional<Predicate<Pair<Job<?>, Task>>> applyTaskIds(Set<String> taskIds) {
        if (taskIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jobAndTask -> taskIds.contains(jobAndTask.getRight().getId()));
    }

    private static Optional<Predicate<Pair<Job<?>, Task>>> applyTaskStates(Set<TaskStatus.TaskState> taskStates) {
        if (taskStates.isEmpty()) {
            return Optional.empty();
        }
        Set<TaskState> coreTaskStates = taskStates.stream().map(V3GrpcModelConverters::toCoreTaskState).collect(Collectors.toSet());
        return Optional.of(jobAndTasks -> {
            Task task = jobAndTasks.getRight();
            return coreTaskStates.contains(task.getStatus().getState());
        });
    }

    private static Optional<Predicate<Pair<Job<?>, Task>>> applyTaskStateReasons(Set<String> taskStateReasons) {
        if (taskStateReasons.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jobAndTasks -> {
            Task task = jobAndTasks.getRight();
            return taskStateReasons.contains(task.getStatus().getReasonCode());
        });
    }

    private static Optional<Predicate<Pair<Job<?>, Task>>> applyNeedsMigration(boolean needsMigration, TitusRuntime titusRuntime) {
        if (!needsMigration) {
            return Optional.empty();
        }
        return Optional.of(jobAndTask -> {
                    Task t = jobAndTask.getRight();
                    if (!JobFunctions.isServiceTask(t)) {
                        return false;
                    }
                    ServiceJobTask serviceTask = (ServiceJobTask) t;
                    titusRuntime.getCodeInvariants().notNull(serviceTask.getMigrationDetails(), "MigrationDetails is null in task: %s", t.getId());
                    return serviceTask.getMigrationDetails() != null && serviceTask.getMigrationDetails().isNeedsMigration();
                }
        );
    }
}
