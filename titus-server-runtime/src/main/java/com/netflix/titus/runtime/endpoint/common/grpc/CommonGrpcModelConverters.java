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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.grpc.protogen.TaskStatus;
import com.netflix.titus.runtime.endpoint.JobQueryCriteria;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static com.netflix.titus.common.util.CollectionsExt.first;
import static com.netflix.titus.common.util.Evaluators.applyNotNull;
import static com.netflix.titus.common.util.StringExt.trimAndApplyIfNonEmpty;
import static java.util.Arrays.asList;

/**
 * Collection of functions to translate between GRPC and Titus common models.
 */
public class CommonGrpcModelConverters {

    public static final String LABEL_LEGACY_NAME = "titus.legacy.name";
    public static final List<TaskStatus.TaskState> ALL_TASK_STATES = asList(TaskStatus.TaskState.values());
    public static final Pattern SPACE_SPLIT_RE = Pattern.compile("\\s+");

    public static final Set<String> CRITERIA_JOB_FIELDS = asSet(
            "jobIds", "taskIds", "owner", "appName", "applicationName", "imageName", "imageTag", "capacityGroup",
            "jobGroupStack", "jobGroupDetail", "jobGroupSequence",
            "jobType", "attributes", "attributes.op", "labels", "labels.op", "jobState", "taskStates", "taskStateReasons",
            "needsMigration"
    );

    public static final Set<String> TASK_CONTEXT_AGENT_ATTRIBUTES = asSet(
            "region", "zone", "asg", "cluster", "stack", "id", "itype"
    );

    public static CallMetadata toCallMetadata(com.netflix.titus.grpc.protogen.CallMetadata grpcCallContext) {
        return CallMetadata.newBuilder()
                .withCallerId(grpcCallContext.getCallerId())
                .withCallReason(grpcCallContext.getCallReason())
                .withCallPath(grpcCallContext.getCallPathList())
                .withDebug(grpcCallContext.getDebug())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.CallMetadata toGrpcCallMetadata(CallMetadata callMetadata) {
        return com.netflix.titus.grpc.protogen.CallMetadata.newBuilder()
                .setCallerId(callMetadata.getCallerId())
                .setCallReason(callMetadata.getCallReason())
                .addAllCallPath(callMetadata.getCallPath())
                .setDebug(callMetadata.isDebug())
                .build();
    }

    public static Page toPage(com.netflix.titus.grpc.protogen.Page grpcPage) {
        return new Page(grpcPage.getPageNumber(), grpcPage.getPageSize(), grpcPage.getCursor());
    }

    public static com.netflix.titus.grpc.protogen.Page toGrpcPage(Page runtimePage) {
        return com.netflix.titus.grpc.protogen.Page.newBuilder()
                .setPageNumber(runtimePage.getPageNumber())
                .setPageSize(runtimePage.getPageSize())
                .setCursor(runtimePage.getCursor())
                .build();
    }

    public static com.netflix.titus.api.model.Pagination toPagination(Pagination grpcPagination) {
        return new com.netflix.titus.api.model.Pagination(
                toPage(grpcPagination.getCurrentPage()),
                grpcPagination.getHasMore(),
                grpcPagination.getTotalPages(),
                grpcPagination.getTotalItems(),
                grpcPagination.getCursor(),
                grpcPagination.getCursorPosition()
        );
    }

    public static Pagination toGrpcPagination(com.netflix.titus.api.model.Pagination runtimePagination) {
        return Pagination.newBuilder()
                .setCurrentPage(toGrpcPage(runtimePagination.getCurrentPage()))
                .setTotalItems(runtimePagination.getTotalItems())
                .setTotalPages(runtimePagination.getTotalPages())
                .setHasMore(runtimePagination.hasMore())
                .setCursor(runtimePagination.getCursor())
                .setCursorPosition(runtimePagination.getCursorPosition())
                .build();
    }

    public static Pagination emptyGrpcPagination(com.netflix.titus.grpc.protogen.Page page) {
        return Pagination.newBuilder()
                .setCurrentPage(page)
                .setTotalItems(0)
                .setTotalPages(0)
                .setHasMore(false)
                .setCursor("")
                .setCursorPosition(0)
                .build();
    }

    public static JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> toJobQueryCriteria(ObserveJobsQuery query) {
        if (query.getFilteringCriteriaCount() == 0) {
            return JobQueryCriteria.<TaskStatus.TaskState, JobSpecCase>newBuilder().build();
        }
        return toJobQueryCriteria(query.getFilteringCriteriaMap());
    }

    public static JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> toJobQueryCriteria(JobQuery jobQuery) {
        if (jobQuery.getFilteringCriteriaCount() == 0) {
            return JobQueryCriteria.<TaskStatus.TaskState, JobSpecCase>newBuilder().build();
        }
        return toJobQueryCriteria(jobQuery.getFilteringCriteriaMap());
    }

    public static JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> toJobQueryCriteria(TaskQuery taskQuery) {
        if (taskQuery.getFilteringCriteriaCount() == 0) {
            return JobQueryCriteria.<TaskStatus.TaskState, JobSpecCase>newBuilder().build();
        }
        return toJobQueryCriteria(taskQuery.getFilteringCriteriaMap());
    }

    private static JobQueryCriteria<TaskStatus.TaskState, JobSpecCase> toJobQueryCriteria(Map<String, String> criteriaMap) {
        JobQueryCriteria.Builder<TaskStatus.TaskState, JobSpecCase> criteriaBuilder = JobQueryCriteria.newBuilder();

        Set<String> unknown = copyAndRemove(criteriaMap.keySet(), CRITERIA_JOB_FIELDS);
        if (!unknown.isEmpty()) {
            throw TitusServiceException.invalidArgument("Unrecognized field(s) " + unknown);
        }

        criteriaBuilder.withJobIds(new HashSet<>(StringExt.splitByComma(criteriaMap.get("jobIds"))));
        criteriaBuilder.withTaskIds(new HashSet<>(StringExt.splitByComma(criteriaMap.get("taskIds"))));
        trimAndApplyIfNonEmpty(criteriaMap.get("owner"), criteriaBuilder::withOwner);
        trimAndApplyIfNonEmpty(criteriaMap.get("appName"), criteriaBuilder::withAppName);
        trimAndApplyIfNonEmpty(criteriaMap.get("applicationName"), criteriaBuilder::withAppName);
        trimAndApplyIfNonEmpty(criteriaMap.get("capacityGroup"), criteriaBuilder::withCapacityGroup);
        trimAndApplyIfNonEmpty(criteriaMap.get("imageName"), criteriaBuilder::withImageName);
        trimAndApplyIfNonEmpty(criteriaMap.get("imageTag"), criteriaBuilder::withImageTag);

        // Job type
        String jobType = criteriaMap.get("jobType");
        if (jobType != null) {
            try {
                criteriaBuilder.withJobType(StringExt.parseEnumIgnoreCase(jobType, JobSpecCase.class));
            } catch (Exception e) {
                throw TitusServiceException.invalidArgument("Invalid jobType value " + jobType);
            }
        }

        // Job group info
        trimAndApplyIfNonEmpty(criteriaMap.get("jobGroupStack"), criteriaBuilder::withJobGroupStack);
        trimAndApplyIfNonEmpty(criteriaMap.get("jobGroupDetail"), criteriaBuilder::withJobGroupDetail);
        trimAndApplyIfNonEmpty(criteriaMap.get("jobGroupSequence"), criteriaBuilder::withJobGroupSequence);

        criteriaBuilder.withNeedsMigration(criteriaMap.getOrDefault("needsMigration", "false").equalsIgnoreCase("true"));

        // Job state
        String jobStateStr = criteriaMap.get("jobState");
        applyNotNull(jobStateStr, js -> criteriaBuilder.withJobState(StringExt.parseEnumIgnoreCase(js, JobStatus.JobState.class)));

        // Task states
        String taskStatesStr = criteriaMap.get("taskStates");
        if (taskStatesStr != null) {
            List<TaskStatus.TaskState> taskStates = StringExt.parseEnumListIgnoreCase(taskStatesStr, TaskStatus.TaskState.class, n -> {
                if (n.equalsIgnoreCase("any")) {
                    return ALL_TASK_STATES;
                }
                return null;
            });
            if (!taskStates.isEmpty()) {
                criteriaBuilder.withTaskStates(new HashSet<>(taskStates));
            }
        }

        // Task reason
        String taskStateReasonsStr = criteriaMap.get("taskStateReasons");
        if (!StringExt.isEmpty(taskStateReasonsStr)) {
            criteriaBuilder.withTaskStateReasons(new HashSet<>(StringExt.splitByComma(taskStateReasonsStr)));
        }

        // Attributes
        String attributeStr = criteriaMap.getOrDefault("attributes", criteriaMap.get("labels"));
        if (attributeStr != null) {
            Map<String, Set<String>> attributes = StringExt.parseKeyValuesList(attributeStr);

            // As we cannot pass null in GRPC, if attribute key contains is single vale which is empty string, assume no value was given
            for (Map.Entry<String, Set<String>> entry : attributes.entrySet()) {
                Set<String> values = entry.getValue();
                if (values.size() == 1 && "".equals(first(values))) {
                    attributes.put(entry.getKey(), Collections.emptySet());
                }
            }

            if (!attributes.isEmpty()) {
                criteriaBuilder.withLabels(attributes);
            }
            String labelsOp = criteriaMap.getOrDefault("attributes.op", criteriaMap.getOrDefault("labels.op", "and")).toLowerCase();
            criteriaBuilder.withLabelsAndOp("and".equals(labelsOp));
        }

        return criteriaBuilder.build();
    }
}
