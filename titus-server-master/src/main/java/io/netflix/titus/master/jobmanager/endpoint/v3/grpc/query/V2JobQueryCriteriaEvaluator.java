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

package io.netflix.titus.master.jobmanager.endpoint.v3.grpc.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.job.JobMgrUtils;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;

public class V2JobQueryCriteriaEvaluator extends V2AbstractQueryCriteriaEvaluator<List<V2WorkerMetadata>> {

    public V2JobQueryCriteriaEvaluator(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria) {
        super(createTaskPredicates(criteria), criteria);
    }

    private static List<Predicate<Pair<V2JobMetadata, List<V2WorkerMetadata>>>> createTaskPredicates(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria) {
        List<Predicate<Pair<V2JobMetadata, List<V2WorkerMetadata>>>> predicates = new ArrayList<>();
        applyTaskIds(criteria.getTaskIds()).ifPresent(predicates::add);
        applyTaskStateAndReasons(criteria.getTaskStates(), criteria.getTaskStateReasons()).ifPresent(predicates::add);
        return predicates;
    }

    private static Optional<Predicate<Pair<V2JobMetadata, List<V2WorkerMetadata>>>> applyTaskIds(Set<String> taskIds) {
        if (taskIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jobAndTasks -> {
            List<V2WorkerMetadata> tasks = jobAndTasks.getRight();
            return tasks.stream().anyMatch(task -> {
                String taskId = WorkerNaming.getTaskId(task);
                return taskIds.contains(taskId) || taskIds.contains(task.getWorkerInstanceId());
            });
        });
    }

    private static Optional<Predicate<Pair<V2JobMetadata, List<V2WorkerMetadata>>>> applyTaskStateAndReasons(Set<TaskStatus.TaskState> taskStates,
                                                                                                             Set<String> taskStateReasons) {

        if (taskStates.isEmpty()) {
            return Optional.empty();
        }

        Pair<Set<V2JobState>, Set<JobCompletedReason>> v2TaskStateAndReasons = toV2TaskStateAndReason(taskStates, taskStateReasons);
        Set<V2JobState> coreTaskStates = v2TaskStateAndReasons.getLeft();
        Set<JobCompletedReason> coreJobReasons = v2TaskStateAndReasons.getRight();

        return Optional.of(jobAndTasks -> {
            List<V2WorkerMetadata> tasks = jobAndTasks.getRight();
            return tasks.stream().anyMatch(t -> {
                if (!coreTaskStates.contains(t.getState()) || JobMgrUtils.isTombStoned(t)) {
                    return false;
                }
                if (coreJobReasons.isEmpty()) {
                    return true;
                }
                return t.getReason() != null && coreJobReasons.contains(t.getReason());
            });
        });
    }
}
