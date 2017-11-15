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

package io.netflix.titus.runtime.endpoint;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.netflix.titus.common.util.CollectionsExt;

import static io.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 * {@link JobQueryCriteria} covers now v2 API search criteria. It must be refactored once v3 query mechanism
 * is finalized.
 */
public class JobQueryCriteria<TASK_STATE, JOB_TYPE extends Enum<JOB_TYPE>> {

    private final Set<String> jobIds;
    private final Set<String> taskIds;
    private final boolean includeArchived;
    private final Optional<Object> jobState;
    private final Set<TASK_STATE> taskStates;
    private final Set<String> taskStateReasons;
    private final Optional<String> owner;
    private final Map<String, Set<String>> labels;
    private final boolean labelsAndOp;
    private final Optional<String> imageName;
    private final Optional<String> imageTag;
    private final Optional<String> appName;
    private final Optional<String> capacityGroup;
    private final Optional<JOB_TYPE> jobType;
    private final Optional<String> jobGroupStack;
    private final Optional<String> jobGroupDetail;
    private final Optional<String> jobGroupSequence;
    private final boolean needsMigration;
    private final int limit;

    private JobQueryCriteria(Set<String> jobIds,
                             Set<String> taskIds,
                             boolean includeArchived,
                             Object jobState,
                             Set<TASK_STATE> taskStates,
                             Set<String> taskStateReasons,
                             String owner,
                             Map<String, Set<String>> labels,
                             boolean labelsAndOp,
                             String imageName,
                             String imageTag,
                             String appName,
                             String capacityGroup,
                             JOB_TYPE jobType,
                             String jobGroupStack,
                             String jobGroupDetail,
                             String jobGroupSequence,
                             boolean needsMigration,
                             int limit) {
        this.jobIds = nonNull(jobIds);
        this.taskIds = nonNull(taskIds);
        this.includeArchived = includeArchived;
        this.jobState = Optional.ofNullable(jobState);
        this.taskStates = CollectionsExt.nonNull(taskStates);
        this.taskStateReasons = CollectionsExt.nonNull(taskStateReasons);
        this.owner = Optional.ofNullable(owner);
        this.labels = labels == null ? Collections.emptyMap() : labels;
        this.labelsAndOp = labelsAndOp;
        this.imageName = Optional.ofNullable(imageName);
        this.imageTag = Optional.ofNullable(imageTag);
        this.appName = Optional.ofNullable(appName);
        this.capacityGroup = Optional.ofNullable(capacityGroup);
        this.jobType = Optional.ofNullable(jobType);
        this.jobGroupStack = Optional.ofNullable(jobGroupStack);
        this.jobGroupDetail = Optional.ofNullable(jobGroupDetail);
        this.jobGroupSequence = Optional.ofNullable(jobGroupSequence);
        this.needsMigration = needsMigration;
        this.limit = limit;
    }

    public static <TASK_STATE, JOB_TYPE extends Enum<JOB_TYPE>> Builder<TASK_STATE, JOB_TYPE> newBuilder() {
        return new Builder<>();
    }

    public Set<String> getJobIds() {
        return jobIds;
    }

    public Set<String> getTaskIds() {
        return taskIds;
    }

    public boolean isIncludeArchived() {
        return includeArchived;
    }

    public Optional<Object> getJobState() {
        return jobState;
    }

    public Set<TASK_STATE> getTaskStates() {
        return taskStates;
    }

    public Set<String> getTaskStateReasons() {
        return taskStateReasons;
    }

    public Optional<String> getOwner() {
        return owner;
    }

    public Map<String, Set<String>> getLabels() {
        return labels;
    }

    public boolean isLabelsAndOp() {
        return labelsAndOp;
    }

    public Optional<String> getImageName() {
        return imageName;
    }

    public Optional<String> getImageTag() {
        return imageTag;
    }

    public Optional<String> getAppName() {
        return appName;
    }

    public Optional<String> getCapacityGroup() {
        return capacityGroup;
    }

    public Optional<JOB_TYPE> getJobType() {
        return jobType;
    }

    public Optional<String> getJobGroupStack() {
        return jobGroupStack;
    }

    public Optional<String> getJobGroupDetail() {
        return jobGroupDetail;
    }

    public Optional<String> getJobGroupSequence() {
        return jobGroupSequence;
    }

    public boolean isNeedsMigration() {
        return needsMigration;
    }

    public int getLimit() {
        return limit;
    }

    public Builder<TASK_STATE, JOB_TYPE> toBuilder() {
        Builder<TASK_STATE, JOB_TYPE> builder = new Builder<>();
        return builder
                .withJobIds(this.jobIds)
                .withTaskIds(this.taskIds)
                .withJobState(this.jobState.orElse(null))
                .withTaskStates(this.taskStates)
                .withOwner(this.owner.orElse(null))
                .withLabels(this.labels)
                .withImageName(this.imageName.orElse(null))
                .withImageTag(this.imageTag.orElse(null))
                .withAppName(this.appName.orElse(null))
                .withCapacityGroup(this.capacityGroup.orElse(null))
                .withJobType(this.jobType.orElse(null))
                .withJobGroupDetail(this.jobGroupDetail.orElse(null))
                .withJobGroupStack(this.jobGroupStack.orElse(null))
                .withJobGroupSequence(this.jobGroupSequence.orElse(null))
                .withNeedsMigration(needsMigration)
                .withLimit(this.limit);
    }

    public boolean isEmpty() {
        return jobIds.isEmpty()
                && taskIds.isEmpty()
                && !jobState.isPresent()
                && taskStates.isEmpty()
                && !owner.isPresent()
                && labels.isEmpty()
                && !imageName.isPresent()
                && !imageTag.isPresent()
                && !appName.isPresent()
                && !capacityGroup.isPresent()
                && !jobType.isPresent()
                && !jobGroupDetail.isPresent()
                && !jobGroupStack.isPresent()
                && !jobGroupSequence.isPresent()
                && !needsMigration
                && limit < 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobQueryCriteria<?, ?> that = (JobQueryCriteria<?, ?>) o;

        if (includeArchived != that.includeArchived) {
            return false;
        }
        if (labelsAndOp != that.labelsAndOp) {
            return false;
        }
        if (needsMigration != that.needsMigration) {
            return false;
        }
        if (limit != that.limit) {
            return false;
        }
        if (jobIds != null ? !jobIds.equals(that.jobIds) : that.jobIds != null) {
            return false;
        }
        if (taskIds != null ? !taskIds.equals(that.taskIds) : that.taskIds != null) {
            return false;
        }
        if (jobState != null ? !jobState.equals(that.jobState) : that.jobState != null) {
            return false;
        }
        if (taskStates != null ? !taskStates.equals(that.taskStates) : that.taskStates != null) {
            return false;
        }
        if (taskStateReasons != null ? !taskStateReasons.equals(that.taskStateReasons) : that.taskStateReasons != null) {
            return false;
        }
        if (owner != null ? !owner.equals(that.owner) : that.owner != null) {
            return false;
        }
        if (labels != null ? !labels.equals(that.labels) : that.labels != null) {
            return false;
        }
        if (imageName != null ? !imageName.equals(that.imageName) : that.imageName != null) {
            return false;
        }
        if (imageTag != null ? !imageTag.equals(that.imageTag) : that.imageTag != null) {
            return false;
        }
        if (appName != null ? !appName.equals(that.appName) : that.appName != null) {
            return false;
        }
        if (capacityGroup != null ? !capacityGroup.equals(that.capacityGroup) : that.capacityGroup != null) {
            return false;
        }
        if (jobType != null ? !jobType.equals(that.jobType) : that.jobType != null) {
            return false;
        }
        if (jobGroupStack != null ? !jobGroupStack.equals(that.jobGroupStack) : that.jobGroupStack != null) {
            return false;
        }
        if (jobGroupDetail != null ? !jobGroupDetail.equals(that.jobGroupDetail) : that.jobGroupDetail != null) {
            return false;
        }
        return jobGroupSequence != null ? jobGroupSequence.equals(that.jobGroupSequence) : that.jobGroupSequence == null;
    }

    @Override
    public int hashCode() {
        int result = jobIds != null ? jobIds.hashCode() : 0;
        result = 31 * result + (taskIds != null ? taskIds.hashCode() : 0);
        result = 31 * result + (includeArchived ? 1 : 0);
        result = 31 * result + (jobState != null ? jobState.hashCode() : 0);
        result = 31 * result + (taskStates != null ? taskStates.hashCode() : 0);
        result = 31 * result + (taskStateReasons != null ? taskStateReasons.hashCode() : 0);
        result = 31 * result + (owner != null ? owner.hashCode() : 0);
        result = 31 * result + (labels != null ? labels.hashCode() : 0);
        result = 31 * result + (labelsAndOp ? 1 : 0);
        result = 31 * result + (imageName != null ? imageName.hashCode() : 0);
        result = 31 * result + (imageTag != null ? imageTag.hashCode() : 0);
        result = 31 * result + (appName != null ? appName.hashCode() : 0);
        result = 31 * result + (capacityGroup != null ? capacityGroup.hashCode() : 0);
        result = 31 * result + (jobType != null ? jobType.hashCode() : 0);
        result = 31 * result + (jobGroupStack != null ? jobGroupStack.hashCode() : 0);
        result = 31 * result + (jobGroupDetail != null ? jobGroupDetail.hashCode() : 0);
        result = 31 * result + (jobGroupSequence != null ? jobGroupSequence.hashCode() : 0);
        result = 31 * result + (needsMigration ? 1 : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "JobQueryCriteria{" +
                "jobIds=" + jobIds +
                ", taskIds=" + taskIds +
                ", includeArchived=" + includeArchived +
                ", jobState=" + jobState +
                ", taskStates=" + taskStates +
                ", taskStateReasons=" + taskStateReasons +
                ", owner=" + owner +
                ", labels=" + labels +
                ", labelsAndOp=" + labelsAndOp +
                ", imageName=" + imageName +
                ", imageTag=" + imageTag +
                ", appName=" + appName +
                ", capacityGroup=" + capacityGroup +
                ", jobType=" + jobType +
                ", jobGroupStack=" + jobGroupStack +
                ", jobGroupDetail=" + jobGroupDetail +
                ", jobGroupSequence=" + jobGroupSequence +
                ", needsMigration=" + needsMigration +
                ", limit=" + limit +
                '}';
    }

    public static final class Builder<TASK_STATE, JOB_TYPE extends Enum<JOB_TYPE>> {
        private Set<String> jobIds;
        private Set<String> taskIds;
        private boolean includeArchived;
        private Object jobState;
        private Set<TASK_STATE> taskStates;
        private Set<String> taskStateReasons;
        private String owner;
        private Map<String, Set<String>> labels;
        private boolean labelsAndOp;
        private String imageName;
        private String imageTag;
        private String appName;
        private String capacityGroup;
        private JOB_TYPE jobType;
        private String jobGroupStack;
        private String jobGroupDetail;
        private String jobGroupSequence;
        private boolean needsMigration;
        private int limit;

        private Builder() {
        }

        public Builder<TASK_STATE, JOB_TYPE> withJobIds(Set<String> jobIds) {
            this.jobIds = jobIds;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withTaskIds(Set<String> taskIds) {
            this.taskIds = taskIds;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withIncludeArchived(boolean includeArchived) {
            this.includeArchived = includeArchived;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withJobState(Object jobState) {
            this.jobState = jobState;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withTaskStates(Set<TASK_STATE> taskStates) {
            this.taskStates = taskStates;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withTaskStateReasons(Set<String> taskStateReasons) {
            this.taskStateReasons = taskStateReasons;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withOwner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withLabels(Map<String, Set<String>> labels) {
            this.labels = labels;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withLabelsAndOp(boolean labelsAndOp) {
            this.labelsAndOp = labelsAndOp;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withImageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withImageTag(String imageTag) {
            this.imageTag = imageTag;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withCapacityGroup(String capacityGroup) {
            this.capacityGroup = capacityGroup;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withJobType(JOB_TYPE jobType) {
            this.jobType = jobType;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withJobGroupStack(String jobGroupStack) {
            this.jobGroupStack = jobGroupStack;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withJobGroupDetail(String jobGroupDetail) {
            this.jobGroupDetail = jobGroupDetail;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withJobGroupSequence(String jobGroupSequence) {
            this.jobGroupSequence = jobGroupSequence;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withNeedsMigration(boolean needsMigration) {
            this.needsMigration = needsMigration;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> withLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder<TASK_STATE, JOB_TYPE> but() {
            return JobQueryCriteria.<TASK_STATE, JOB_TYPE>newBuilder()
                    .withJobIds(jobIds)
                    .withIncludeArchived(includeArchived)
                    .withJobState(jobState)
                    .withTaskStates(taskStates)
                    .withTaskStateReasons(taskStateReasons)
                    .withLabels(labels)
                    .withLabelsAndOp(labelsAndOp)
                    .withImageName(imageName)
                    .withAppName(appName)
                    .withJobType(jobType)
                    .withNeedsMigration(needsMigration)
                    .withLimit(limit);
        }

        public JobQueryCriteria<TASK_STATE, JOB_TYPE> build() {
            return new JobQueryCriteria<>(jobIds, taskIds, includeArchived, jobState, taskStates, taskStateReasons, owner, labels,
                    labelsAndOp, imageName, imageTag, appName, capacityGroup, jobType, jobGroupStack, jobGroupDetail, jobGroupSequence,
                    needsMigration, limit);
        }
    }
}
