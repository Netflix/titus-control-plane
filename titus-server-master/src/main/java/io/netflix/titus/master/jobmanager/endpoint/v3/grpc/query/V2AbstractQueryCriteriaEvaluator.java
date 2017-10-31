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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.model.v2.parameter.Parameters.JobType;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.endpoint.v3.grpc.gateway.V2GrpcModelConverters;
import io.netflix.titus.runtime.endpoint.JobQueryCriteria;
import io.netflix.titus.runtime.endpoint.common.QueryUtils;

public class V2AbstractQueryCriteriaEvaluator<TASK_OR_SET> implements Predicate<Pair<V2JobMetadata, TASK_OR_SET>> {

    private final Predicate<Pair<V2JobMetadata, TASK_OR_SET>> queryPredicate;

    protected V2AbstractQueryCriteriaEvaluator(List<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> taskPredicates,
                                               JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria) {
        List<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> predicates = createJobPredicates(criteria);
        predicates.addAll(taskPredicates);
        this.queryPredicate = matchAll(predicates);
    }

    @Override
    public boolean test(Pair<V2JobMetadata, TASK_OR_SET> jobListPair) {
        return queryPredicate.test(jobListPair);
    }

    protected static Pair<Set<V2JobState>, Set<JobCompletedReason>> toV2TaskStateAndReason(Set<TaskStatus.TaskState> taskStates,
                                                                                           Set<String> taskStateReasons) {
        if (taskStates.isEmpty()) {
            return Pair.of(Collections.emptySet(), Collections.emptySet());
        }

        Set<JobCompletedReason> coreJobReasons = taskStateReasons.stream()
                .map(V2GrpcModelConverters::toV2JobCompletedReason)
                .collect(Collectors.toSet());
        Set<JobCompletedReason> jobReasonsExceptNormal = CollectionsExt.copyAndRemove(coreJobReasons, JobCompletedReason.Normal);

        Set<V2JobState> coreTaskStates = taskStates.stream()
                .filter(taskState -> taskState != TaskStatus.TaskState.KillInitiated)
                .flatMap((TaskStatus.TaskState grpcTaskState) -> {
                    V2JobState v2JobState = V2GrpcModelConverters.toV2JobState(grpcTaskState);
                    if (v2JobState == V2JobState.Completed) {
                        Set<V2JobState> finishedStates = new HashSet<>();
                        if (coreJobReasons.isEmpty()) {
                            finishedStates.add(V2JobState.Completed);
                            finishedStates.add(V2JobState.Failed);
                        } else {
                            if (coreJobReasons.contains(JobCompletedReason.Normal)) {
                                finishedStates.add(V2JobState.Completed);
                            }
                            if (!jobReasonsExceptNormal.isEmpty()) {
                                finishedStates.add(V2JobState.Failed);
                            }
                        }
                        return finishedStates.stream();
                    }
                    return Stream.of(v2JobState);
                })
                .collect(Collectors.toSet());

        return Pair.of(coreTaskStates, coreJobReasons);
    }

    private Predicate<Pair<V2JobMetadata, TASK_OR_SET>> matchAll(List<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> predicates) {
        return jobAndTasks -> {
            for (Predicate<Pair<V2JobMetadata, TASK_OR_SET>> predicate : predicates) {
                if (!predicate.test(jobAndTasks)) {
                    return false;
                }
            }
            return true;
        };
    }

    private List<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> createJobPredicates(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria) {
        List<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> predicates = new ArrayList<>();

        applyJobIds(criteria.getJobIds()).ifPresent(predicates::add);
        applyJobType(criteria.getJobType()).ifPresent(predicates::add);
        applyOwner(criteria.getOwner()).ifPresent(predicates::add);
        applyApplicationName(criteria.getAppName()).ifPresent(predicates::add);
        applyCapacityGroup(criteria.getCapacityGroup()).ifPresent(predicates::add);
        applyJobGroupStack(criteria.getJobGroupStack()).ifPresent(predicates::add);
        applyJobGroupDetail(criteria.getJobGroupDetail()).ifPresent(predicates::add);
        applyJobGroupSequence(criteria.getJobGroupSequence()).ifPresent(predicates::add);
        applyImageName(criteria.getImageName()).ifPresent(predicates::add);
        applyImageTag(criteria.getImageTag()).ifPresent(predicates::add);
        applyJobDescriptorAttributes(criteria.getLabels(), criteria.isLabelsAndOp()).ifPresent(predicates::add);

        return predicates;
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyJobType(Optional<JobDescriptor.JobSpecCase> jobTypeOpt) {
        return jobTypeOpt.map(jobType ->
                jobTaskPair -> {
                    JobType currentJobType = Parameters.getJobType(jobTaskPair.getLeft().getParameters());
                    return (currentJobType == JobType.Batch && jobType.equals(JobDescriptor.JobSpecCase.BATCH))
                            || (currentJobType == JobType.Service && jobType.equals(JobDescriptor.JobSpecCase.SERVICE));
                }
        );
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyJobIds(Set<String> jobIds) {
        return jobIds.isEmpty() ? Optional.empty() : Optional.of(jobAndTasks -> jobIds.contains(jobAndTasks.getLeft().getJobId()));
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyOwner(Optional<String> ownerOpt) {
        return ownerOpt.map(owner ->
                jobTaskPair -> {
                    String currentOwner = jobTaskPair.getLeft().getUser();
                    return currentOwner != null && currentOwner.equals(owner);
                }
        );
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyApplicationName(Optional<String> appNameOpt) {
        return appNameOpt.map(appName ->
                jobTaskPair -> {
                    String currentAppName = Parameters.getAppName(jobTaskPair.getLeft().getParameters());
                    return currentAppName != null && currentAppName.equals(appName);
                }
        );
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyCapacityGroup(Optional<String> capacityGroupOpt) {
        return capacityGroupOpt.map(appName ->
                jobTaskPair -> {
                    String currentCapacityGroup = Parameters.getCapacityGroup(jobTaskPair.getLeft().getParameters());
                    return currentCapacityGroup != null && currentCapacityGroup.equals(appName);
                }
        );
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyJobGroupStack(Optional<String> jobGroupStack) {
        return apply(jobGroupStack, job -> Parameters.getJobGroupStack(job.getParameters()));
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyJobGroupDetail(Optional<String> jobGroupDetail) {
        return apply(jobGroupDetail, job -> Parameters.getJobGroupDetail(job.getParameters()));
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyJobGroupSequence(Optional<String> jobGroupSequence) {
        return apply(jobGroupSequence, job -> Parameters.getJobGroupSeq(job.getParameters()));
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyImageName(Optional<String> imageName) {
        return apply(imageName, job -> Parameters.getImageName(job.getParameters()));
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyImageTag(Optional<String> imageTag) {
        return apply(imageTag, job -> Parameters.getVersion(job.getParameters()));
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> apply(Optional<String> expectedOpt, Function<V2JobMetadata, String> valueGetter) {
        return expectedOpt.map(expected ->
                jobTaskPair -> {
                    String actual = valueGetter.apply(jobTaskPair.getLeft());
                    return actual != null && actual.equals(expected);
                }
        );
    }

    private Optional<Predicate<Pair<V2JobMetadata, TASK_OR_SET>>> applyJobDescriptorAttributes(Map<String, Set<String>> attributes, boolean andOperator) {
        if (attributes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jobTaskPair -> {
                    Map<String, String> jobLabels = Parameters.getLabels(jobTaskPair.getLeft().getParameters());
                    return QueryUtils.matchesAttributes(attributes, jobLabels, andOperator);
                }
        );
    }
}
