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

package com.netflix.titus.supplementary.relocation.endpoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;

import static com.netflix.titus.common.util.Evaluators.acceptNotNull;
import static com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters.toGrpcTaskRelocationPlans;

public class TaskRelocationPlanPredicate implements Predicate<TaskRelocationPlan> {

    public static final String FILTER_JOB_IDS = "jobIds";

    public static final String FILTER_TASK_IDS = "taskIds";

    public static final String FILTER_APPLICATION_NAME = "applicationName";

    public static final String FILTER_CAPACITY_GROUP = "capacityGroup";

    private final ReadOnlyJobOperations jobOperations;
    private final Predicate<TaskRelocationPlan> predicate;

    @VisibleForTesting
    public TaskRelocationPlanPredicate(ReadOnlyJobOperations jobOperations, TaskRelocationQuery request) {
        this.jobOperations = jobOperations;
        Map<String, String> criteria = request.getFilteringCriteriaMap();
        if (criteria.isEmpty()) {
            this.predicate = relocationPlan -> true;
        } else {
            List<Predicate<TaskRelocationPlan>> predicates = new ArrayList<>();

            acceptNotNull(criteria.get(FILTER_JOB_IDS), value -> newJobIdsPredicate(value).ifPresent(predicates::add));
            acceptNotNull(criteria.get(FILTER_TASK_IDS), value -> newTaskIdsPredicate(value).ifPresent(predicates::add));
            acceptNotNull(criteria.get(FILTER_APPLICATION_NAME), value -> newApplicationNamePredicate(value).ifPresent(predicates::add));
            acceptNotNull(criteria.get(FILTER_CAPACITY_GROUP), value -> newCapacityGroupPredicate(value).ifPresent(predicates::add));

            if (predicates.isEmpty()) {
                this.predicate = relocationPlan -> true;
            } else {
                this.predicate = relocationPlan -> {
                    for (Predicate<TaskRelocationPlan> predicate : predicates) {
                        if (!predicate.test(relocationPlan)) {
                            return false;
                        }
                    }
                    return true;
                };
            }
        }
    }

    @Override
    public boolean test(TaskRelocationPlan relocationPlan) {
        return predicate.test(relocationPlan);
    }

    private Optional<Predicate<TaskRelocationPlan>> newJobIdsPredicate(String jobIds) {
        List<String> idList = StringExt.splitByComma(jobIds);
        if (idList.isEmpty()) {
            return Optional.empty();
        }
        Set<String> idSet = new HashSet<>(idList);

        return Optional.of(newJobPredicate(job -> idSet.contains(job.getId())));
    }

    private Optional<Predicate<TaskRelocationPlan>> newTaskIdsPredicate(String taskIds) {
        List<String> idList = StringExt.splitByComma(taskIds);
        if (idList.isEmpty()) {
            return Optional.empty();
        }
        Set<String> idSet = new HashSet<>(idList);

        return Optional.of(taskRelocationPlan -> idSet.contains(taskRelocationPlan.getTaskId()));
    }

    private Optional<Predicate<TaskRelocationPlan>> newApplicationNamePredicate(String applicationName) {
        if (applicationName.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(newJobPredicate(job -> applicationName.equalsIgnoreCase(job.getJobDescriptor().getApplicationName())));
    }

    private Optional<Predicate<TaskRelocationPlan>> newCapacityGroupPredicate(String capacityGroup) {
        if (capacityGroup.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(newJobPredicate(job -> capacityGroup.equalsIgnoreCase(job.getJobDescriptor().getCapacityGroup())));
    }

    private Predicate<TaskRelocationPlan> newJobPredicate(Predicate<Job<?>> jobPredicate) {
        return taskRelocationPlan -> {
            Optional<Pair<Job<?>, Task>> jobTaskOpt = jobOperations.findTaskById(taskRelocationPlan.getTaskId());
            if (!jobTaskOpt.isPresent()) {
                return false;
            }
            return jobPredicate.test(jobTaskOpt.get().getLeft());
        };
    }

    public static TaskRelocationPlans buildProtobufQueryResult(ReadOnlyJobOperations jobOperations,
                                                               RelocationWorkflowExecutor relocationWorkflowExecutor,
                                                               TaskRelocationQuery request) {
        Predicate<TaskRelocationPlan> filter = new TaskRelocationPlanPredicate(jobOperations, request);
        List<TaskRelocationPlan> corePlans = new ArrayList<>(relocationWorkflowExecutor.getPlannedRelocations().values());
        List<TaskRelocationPlan> filtered = corePlans.stream().filter(filter).collect(Collectors.toList());
        return toGrpcTaskRelocationPlans(filtered);
    }
}
