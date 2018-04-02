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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.endpoint.common.QueryUtils;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;

public abstract class V3AbstractQueryCriteriaEvaluator<TASK_OR_SET> implements Predicate<Pair<Job<?>, TASK_OR_SET>> {

    private final Predicate<Pair<Job<?>, TASK_OR_SET>> queryPredicate;

    protected V3AbstractQueryCriteriaEvaluator(List<Predicate<Pair<Job<?>, TASK_OR_SET>>> taskPredicates,
                                               JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria) {
        List<Predicate<Pair<Job<?>, TASK_OR_SET>>> predicates = createJobPredicates(criteria);
        predicates.addAll(taskPredicates);
        this.queryPredicate = matchAll(predicates);
    }

    @Override
    public boolean test(Pair<Job<?>, TASK_OR_SET> jobListPair) {
        return queryPredicate.test(jobListPair);
    }

    private List<Predicate<Pair<Job<?>, TASK_OR_SET>>> createJobPredicates(JobQueryCriteria<TaskStatus.TaskState, JobDescriptor.JobSpecCase> criteria) {
        List<Predicate<Pair<Job<?>, TASK_OR_SET>>> predicates = new ArrayList<>();

        applyJobIds(criteria.getJobIds()).ifPresent(predicates::add);
        applyJobType(criteria.getJobType()).ifPresent(predicates::add);
        applyJobState(criteria.getJobState()).ifPresent(predicates::add);
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

    private Predicate<Pair<Job<?>, TASK_OR_SET>> matchAll(List<Predicate<Pair<Job<?>, TASK_OR_SET>>> predicates) {
        return jobAndTasks -> {
            for (Predicate<Pair<Job<?>, TASK_OR_SET>> predicate : predicates) {
                if (!predicate.test(jobAndTasks)) {
                    return false;
                }
            }
            return true;
        };
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobType(Optional<JobDescriptor.JobSpecCase> jobTypeOpt) {
        return jobTypeOpt.map(jobType ->
                jobTaskPair -> {
                    boolean isBatchJob = jobTaskPair.getLeft().getJobDescriptor().getExtensions() instanceof BatchJobExt;
                    return (isBatchJob && jobType.equals(JobDescriptor.JobSpecCase.BATCH)) || (!isBatchJob && jobType.equals(JobDescriptor.JobSpecCase.SERVICE));
                }
        );
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobIds(Set<String> jobIds) {
        return jobIds.isEmpty() ? Optional.empty() : Optional.of(jobAndTasks -> jobIds.contains(jobAndTasks.getLeft().getId()));
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobState(Optional<Object> jobStateOpt) {
        return jobStateOpt.map(jobStateObj -> {
            JobState jobState = V3GrpcModelConverters.toCoreJobState((JobStatus.JobState) jobStateObj);
            return jobTaskPair -> jobTaskPair.getLeft().getStatus().getState().equals(jobState);
        });
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyOwner(Optional<String> ownerOpt) {
        return ownerOpt.map(owner ->
                jobTaskPair -> jobTaskPair.getLeft().getJobDescriptor().getOwner().getTeamEmail().equals(owner)
        );
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyApplicationName(Optional<String> appNameOpt) {
        return appNameOpt.map(appName ->
                jobTaskPair -> {
                    String currentAppName = jobTaskPair.getLeft().getJobDescriptor().getApplicationName();
                    return currentAppName != null && currentAppName.equals(appName);
                }
        );
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyCapacityGroup(Optional<String> capacityGroupOpt) {
        return capacityGroupOpt.map(capacityGroup ->
                jobTaskPair -> {
                    String currentCapacityGroup = jobTaskPair.getLeft().getJobDescriptor().getCapacityGroup();
                    return currentCapacityGroup != null && currentCapacityGroup.equals(capacityGroup);
                }
        );
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobGroupStack(Optional<String> jobGroupStack) {
        return apply(jobGroupStack, job -> {
            JobGroupInfo jobGroupInfo = job.getJobDescriptor().getJobGroupInfo();
            return jobGroupInfo == null ? null : jobGroupInfo.getStack();
        });
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobGroupDetail(Optional<String> jobGroupDetail) {
        return apply(jobGroupDetail, job -> {
            JobGroupInfo jobGroupInfo = job.getJobDescriptor().getJobGroupInfo();
            return jobGroupInfo == null ? null : jobGroupInfo.getDetail();
        });
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobGroupSequence(Optional<String> jobGroupSequence) {
        return apply(jobGroupSequence, job -> {
            JobGroupInfo jobGroupInfo = job.getJobDescriptor().getJobGroupInfo();
            return jobGroupInfo == null ? null : jobGroupInfo.getSequence();
        });
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyImageName(Optional<String> imageName) {
        return apply(imageName, job -> job.getJobDescriptor().getContainer().getImage().getName());
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyImageTag(Optional<String> imageTag) {
        return apply(imageTag, job -> job.getJobDescriptor().getContainer().getImage().getTag());
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> apply(Optional<String> expectedOpt, Function<Job<?>, String> valueGetter) {
        return expectedOpt.map(expected ->
                jobTaskPair -> {
                    String actual = valueGetter.apply(jobTaskPair.getLeft());
                    return actual != null && actual.equals(expected);
                }
        );
    }

    private Optional<Predicate<Pair<Job<?>, TASK_OR_SET>>> applyJobDescriptorAttributes(Map<String, Set<String>> attributes, boolean andOperator) {
        if (attributes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jobTaskPair -> {
                    Map<String, String> jobAttributes = jobTaskPair.getLeft().getJobDescriptor().getAttributes();
                    return QueryUtils.matchesAttributes(attributes, jobAttributes, andOperator);
                }
        );
    }
}
