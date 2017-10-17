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

package io.netflix.titus.api.jobmanager.model.job;

import java.util.Collections;
import java.util.List;
import javax.validation.Valid;

import io.netflix.titus.common.model.sanitizer.NeverNull;
import io.netflix.titus.common.util.CollectionsExt;

import static io.netflix.titus.common.util.CollectionsExt.nonNull;
import static java.util.Arrays.asList;

/**
 * Immutable entity that represents an information about a Titus job in a distinct point in time.
 * Both job descriptor and job status may change over time, and for each change a new version of the {@link Job}
 * entity is created. For example, service job can be re-sized ({@link JobDescriptor} update), or change state from
 * {@link JobState#Accepted} to {@link JobState#Finished}.
 */
@NeverNull
public class Job<E extends JobDescriptor.JobDescriptorExt> {

    private final String id;

    @Valid
    private final JobDescriptor<E> jobDescriptor;

    private final JobStatus status;

    private final List<JobStatus> statusHistory;

    public Job(String id, JobDescriptor<E> jobDescriptor, JobStatus status, List<JobStatus> statusHistory) {
        this.id = id;
        this.jobDescriptor = jobDescriptor;
        this.status = status;
        this.statusHistory = CollectionsExt.nullableImmutableCopyOf(statusHistory);
    }

    /**
     * A unique id (UUID) of a job, fixed for the job lifetime.
     */
    public String getId() {
        return id;
    }

    /**
     * Job descriptor.
     */
    public JobDescriptor<E> getJobDescriptor() {
        return jobDescriptor;
    }

    /**
     * Last known job state.
     */
    public JobStatus getStatus() {
        return status;
    }

    /**
     * The history of previous statuses
     */
    public List<JobStatus> getStatusHistory() {
        return statusHistory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Job<?> job = (Job<?>) o;

        if (id != null ? !id.equals(job.id) : job.id != null) {
            return false;
        }
        if (jobDescriptor != null ? !jobDescriptor.equals(job.jobDescriptor) : job.jobDescriptor != null) {
            return false;
        }
        if (status != null ? !status.equals(job.status) : job.status != null) {
            return false;
        }
        return statusHistory != null ? statusHistory.equals(job.statusHistory) : job.statusHistory == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (jobDescriptor != null ? jobDescriptor.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (statusHistory != null ? statusHistory.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Job{" +
                "id='" + id + '\'' +
                ", jobDescriptor=" + jobDescriptor +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                '}';
    }

    public Builder<E> toBuilder() {
        return newBuilder(this);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Builder<E> newBuilder() {
        return new Builder<>();
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Builder<E> newBuilder(Job<E> job) {
        return new Builder<E>()
                .withId(job.getId())
                .withJobDescriptor(job.getJobDescriptor())
                .withStatus(job.getStatus());
    }

    public static final class Builder<E extends JobDescriptor.JobDescriptorExt> {
        private String id;
        private JobDescriptor<E> jobDescriptor;
        private JobStatus status;
        private List<JobStatus> statusHistory;

        private Builder() {
        }

        public Builder<E> withId(String id) {
            this.id = id;
            return this;
        }

        public Builder<E> withJobDescriptor(JobDescriptor<E> jobDescriptor) {
            this.jobDescriptor = jobDescriptor;
            return this;
        }

        public Builder<E> withStatus(JobStatus status) {
            this.status = status;
            return this;
        }

        public Builder<E> withStatusHistory(List<JobStatus> statusHistory) {
            this.statusHistory = statusHistory;
            return this;
        }

        public Builder<E> withStatusHistory(JobStatus... statusHistory) {
            if (statusHistory.length == 0) {
                return withStatusHistory(Collections.emptyList());
            }
            if (statusHistory.length == 1) {
                return withStatusHistory(Collections.singletonList(statusHistory[0]));
            }
            return withStatusHistory(asList(statusHistory));
        }

        public Job<E> build() {
            return new Job<>(id, jobDescriptor, status, nonNull(statusHistory));
        }
    }
}
