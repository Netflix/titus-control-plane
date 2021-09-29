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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.validation.Valid;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;
import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;
import static java.util.Arrays.asList;

/**
 * Immutable entity that represents an information about a Titus job in a distinct point in time.
 * Both job descriptor and job status may change over time, and for each change a new version of the {@link Job}
 * entity is created. For example, service job can be re-sized ({@link JobDescriptor} update), or change state from
 * {@link JobState#Accepted} to {@link JobState#Finished}.
 */
@ClassFieldsNotNull
public class Job<E extends JobDescriptor.JobDescriptorExt> {

    private final String id;

    @Valid
    private final JobDescriptor<E> jobDescriptor;

    private final JobStatus status;

    private final List<JobStatus> statusHistory;

    private final Version version;

    public Job(String id, JobDescriptor<E> jobDescriptor, JobStatus status, List<JobStatus> statusHistory, Version version) {
        this.id = id;
        this.jobDescriptor = jobDescriptor;
        this.status = status;
        this.statusHistory = CollectionsExt.nullableImmutableCopyOf(statusHistory);
        // TODO Remove me. Version is a newly introduced field. If not set explicitly, assign a default value
        this.version = version == null ? Version.undefined() : version;
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

    public Version getVersion() {
        return version;
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
        return Objects.equals(id, job.id) &&
                Objects.equals(jobDescriptor, job.jobDescriptor) &&
                Objects.equals(status, job.status) &&
                Objects.equals(statusHistory, job.statusHistory) &&
                Objects.equals(version, job.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobDescriptor, status, statusHistory, version);
    }

    @Override
    public String toString() {
        return "Job{" +
                "id='" + id + '\'' +
                ", jobDescriptor=" + jobDescriptor +
                ", status=" + status +
                ", statusHistory=" + statusHistory +
                ", version=" + version +
                '}';
    }

    @SafeVarargs
    public final Job<E> but(Function<Job<E>, Job<E>>... modifiers) {
        Job<E> result = this;
        for (Function<Job<E>, Job<E>> modifier : modifiers) {
            result = modifier.apply(result);
        }
        return result;
    }

    public Builder<E> toBuilder() {
        return new Builder<E>()
                .withId(id)
                .withJobDescriptor(jobDescriptor)
                .withStatus(status)
                .withStatusHistory(statusHistory)
                .withVersion(version);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Builder<E> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<E extends JobDescriptor.JobDescriptorExt> {
        private String id;
        private JobDescriptor<E> jobDescriptor;
        private JobStatus status;
        private List<JobStatus> statusHistory;
        private Version version;

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

        public Builder<E> withVersion(Version version) {
            this.version = version;
            return this;
        }

        public Job<E> build() {
            return new Job<>(id, jobDescriptor, status, nonNull(statusHistory), version);
        }
    }
}
