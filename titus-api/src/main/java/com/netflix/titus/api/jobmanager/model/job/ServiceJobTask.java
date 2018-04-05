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

package com.netflix.titus.api.jobmanager.model.job;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.migration.MigrationDetails;
import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 */
@ClassFieldsNotNull
public class ServiceJobTask extends Task {

    private final MigrationDetails migrationDetails;

    public ServiceJobTask(String id,
                          String originalId,
                          Optional<String> resubmitOf,
                          String jobId,
                          int resubmitNumber,
                          int systemResubmitNumber,
                          TaskStatus status,
                          List<TaskStatus> statusHistory,
                          List<TwoLevelResource> twoLevelResources,
                          Map<String, String> taskContext,
                          MigrationDetails migrationDetails) {
        super(id, jobId, status, statusHistory, originalId, resubmitOf, resubmitNumber, systemResubmitNumber, twoLevelResources, taskContext);
        this.migrationDetails = migrationDetails;
    }

    public MigrationDetails getMigrationDetails() {
        return migrationDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ServiceJobTask that = (ServiceJobTask) o;

        return migrationDetails != null ? migrationDetails.equals(that.migrationDetails) : that.migrationDetails == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (migrationDetails != null ? migrationDetails.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ServiceJobTask{" +
                "id='" + getId() + '\'' +
                ", jobId='" + getJobId() + '\'' +
                ", status=" + getStatus() +
                ", originalId='" + getOriginalId() + '\'' +
                ", resubmitOf=" + getResubmitOf() +
                ", resubmitNumber=" + getResubmitNumber() +
                ", systemResubmitNumber=" + getSystemResubmitNumber() +
                ", twoLevelResources=" + getTwoLevelResources() +
                ", taskContext=" + getTaskContext() +
                "migrationDetails=" + migrationDetails +
                '}';
    }

    @Override
    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ServiceJobTask serviceJobTask) {
        return new Builder(serviceJobTask);
    }

    public static class Builder extends TaskBuilder<ServiceJobTask, Builder> {

        private MigrationDetails migrationDetails;

        private Builder() {
        }

        private Builder(ServiceJobTask serviceJobTask) {
            newBuilder(this, serviceJobTask);
        }

        public Builder withMigrationDetails(MigrationDetails migrationDetails) {
            this.migrationDetails = migrationDetails;
            return this;
        }

        public Builder but() {
            return but(new Builder().withMigrationDetails(migrationDetails));
        }

        @Override
        public ServiceJobTask build() {
            migrationDetails = migrationDetails == null ? new MigrationDetails(false, 0) : migrationDetails;
            return new ServiceJobTask(id,
                    originalId,
                    Optional.ofNullable(resubmitOf),
                    jobId,
                    resubmitNumber,
                    systemResubmitNumber,
                    status,
                    nonNull(statusHistory),
                    nonNull(twoLevelResources),
                    nonNull(taskContext),
                    migrationDetails
            );
        }
    }
}
