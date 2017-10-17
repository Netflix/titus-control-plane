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

import java.util.Map;
import java.util.function.Function;
import javax.validation.Valid;
import javax.validation.constraints.Size;

import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.common.model.sanitizer.ClassInvariant;
import io.netflix.titus.common.model.sanitizer.CollectionInvariants;
import io.netflix.titus.common.model.sanitizer.NeverNull;
import io.netflix.titus.common.model.sanitizer.Template;
import io.netflix.titus.common.util.CollectionsExt;

/**
 */
@NeverNull
@ClassInvariant(expr = "@asserts.notExceedsComputeResources(capacityGroup, container)")
public class JobDescriptor<E extends JobDescriptor.JobDescriptorExt> {

    /**
     * A marker interface for {@link JobDescriptor} extensions.
     */
    public interface JobDescriptorExt {
    }

    @Valid
    private final Owner owner;

    @Size(min = 1, message = "Empty string not allowed")
    private final String applicationName;

    @Template
    private final String capacityGroup;

    @Valid
    private final JobGroupInfo jobGroupInfo;

    @CollectionInvariants
    private final Map<String, String> attributes;

    @Valid
    private final Container container;

    @Valid
    private final E extensions;

    public JobDescriptor(Owner owner,
                         String applicationName,
                         String capacityGroup,
                         JobGroupInfo jobGroupInfo,
                         Map<String, String> attributes,
                         Container container,
                         E extensions) {
        this.owner = owner;
        this.applicationName = applicationName;
        this.capacityGroup = capacityGroup;
        this.jobGroupInfo = jobGroupInfo;
        this.attributes = CollectionsExt.nullableImmutableCopyOf(attributes);
        this.container = container;
        this.extensions = extensions;
    }

    /**
     * Owner of a job (see Owner entity description for more information).
     */
    public Owner getOwner() {
        return owner;
    }

    /**
     * Arbitrary name, not interpreted by Titus. Does not have to be unique. If not provided, a default
     * name, that depends on job type (batch or service) is set.
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Capacity group associated with a job.
     */
    public String getCapacityGroup() {
        return capacityGroup;
    }

    /**
     * Mostly relevant for service jobs, but applicable to batch jobs as well, provides further grouping
     * criteria for a job.
     */
    public JobGroupInfo getJobGroupInfo() {
        return jobGroupInfo;
    }

    /**
     * Arbitrary set of key/value pairs. Names starting with 'titus' (case does not matter) are reserved for internal use.
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    /**
     * Container to be executed for a job.
     */
    public Container getContainer() {
        return container;
    }

    /**
     * Returns job type specific data.
     */
    public E getExtensions() {
        return extensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobDescriptor<?> that = (JobDescriptor<?>) o;

        if (owner != null ? !owner.equals(that.owner) : that.owner != null) {
            return false;
        }
        if (applicationName != null ? !applicationName.equals(that.applicationName) : that.applicationName != null) {
            return false;
        }
        if (capacityGroup != null ? !capacityGroup.equals(that.capacityGroup) : that.capacityGroup != null) {
            return false;
        }
        if (jobGroupInfo != null ? !jobGroupInfo.equals(that.jobGroupInfo) : that.jobGroupInfo != null) {
            return false;
        }
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {
            return false;
        }
        if (container != null ? !container.equals(that.container) : that.container != null) {
            return false;
        }
        return extensions != null ? extensions.equals(that.extensions) : that.extensions == null;
    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (applicationName != null ? applicationName.hashCode() : 0);
        result = 31 * result + (capacityGroup != null ? capacityGroup.hashCode() : 0);
        result = 31 * result + (jobGroupInfo != null ? jobGroupInfo.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (container != null ? container.hashCode() : 0);
        result = 31 * result + (extensions != null ? extensions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JobDescriptor{" +
                "owner=" + owner +
                ", applicationName='" + applicationName + '\'' +
                ", capacityGroup='" + capacityGroup + '\'' +
                ", jobGroupInfo=" + jobGroupInfo +
                ", attributes=" + attributes +
                ", container=" + container +
                ", extensions=" + extensions +
                '}';
    }

    public JobDescriptor<E> but(Function<JobDescriptor<E>, Object> mapperFun) {
        Object result = mapperFun.apply(this);
        if (result instanceof JobDescriptor) {
            return (JobDescriptor<E>) result;
        }
        if (result instanceof JobDescriptor.Builder) {
            return ((JobDescriptor.Builder<E>) result).build();
        }
        if (result instanceof Owner) {
            return toBuilder().withOwner((Owner) result).build();
        }
        if (result instanceof Owner.Builder) {
            return toBuilder().withOwner(((Owner.Builder) result).build()).build();
        }
        if (result instanceof JobGroupInfo) {
            return toBuilder().withJobGroupInfo((JobGroupInfo) result).build();
        }
        if (result instanceof JobGroupInfo.Builder) {
            return toBuilder().withJobGroupInfo(((JobGroupInfo.Builder) result).build()).build();
        }
        if (result instanceof Container) {
            return toBuilder().withContainer((Container) result).build();
        }
        if (result instanceof Container.Builder) {
            return toBuilder().withContainer(((Container.Builder) result).build()).build();
        }
        if (result instanceof JobDescriptorExt) {
            return toBuilder().withExtensions((E) result).build();
        }
        if (result instanceof BatchJobExt.Builder) {
            return toBuilder().withExtensions((E) ((BatchJobExt.Builder) result).build()).build();
        }
        if (result instanceof ServiceJobExt.Builder) {
            return toBuilder().withExtensions((E) ((ServiceJobExt.Builder) result).build()).build();
        }
        throw new IllegalArgumentException("Invalid result type " + result.getClass());
    }

    public Builder<E> toBuilder() {
        return newBuilder(this);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Builder<E> newBuilder() {
        return new Builder<>();
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Builder<E> newBuilder(JobDescriptor<E> jobDescriptor) {
        return new Builder<E>()
                .withOwner(jobDescriptor.getOwner())
                .withApplicationName(jobDescriptor.getApplicationName())
                .withCapacityGroup(jobDescriptor.getCapacityGroup())
                .withJobGroupInfo(jobDescriptor.getJobGroupInfo())
                .withAttributes(jobDescriptor.getAttributes())
                .withContainer(jobDescriptor.getContainer())
                .withExtensions(jobDescriptor.getExtensions());
    }

    public static final class Builder<E extends JobDescriptor.JobDescriptorExt> {
        private Owner owner;
        private String applicationName;
        private String capacityGroup;
        private JobGroupInfo jobGroupInfo;
        private Map<String, String> attributes;
        private Container container;
        private E extensions;

        private Builder() {
        }

        public Builder<E> withOwner(Owner owner) {
            this.owner = owner;
            return this;
        }

        public Builder<E> withApplicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public Builder<E> withCapacityGroup(String capacityGroup) {
            this.capacityGroup = capacityGroup;
            return this;
        }

        public Builder<E> withJobGroupInfo(JobGroupInfo jobGroupInfo) {
            this.jobGroupInfo = jobGroupInfo;
            return this;
        }

        public Builder<E> withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder<E> withContainer(Container container) {
            this.container = container;
            return this;
        }

        public Builder<E> withExtensions(E extensions) {
            this.extensions = extensions;
            return this;
        }

        public Builder<E> but() {
            return new Builder<E>()
                    .withOwner(owner)
                    .withApplicationName(applicationName)
                    .withCapacityGroup(capacityGroup)
                    .withJobGroupInfo(jobGroupInfo)
                    .withAttributes(attributes)
                    .withContainer(container)
                    .withExtensions(extensions);
        }

        public JobDescriptor<E> build() {
            JobDescriptor<E> jobDescriptor = new JobDescriptor<>(owner, applicationName, capacityGroup, jobGroupInfo, attributes, container, extensions);
            return jobDescriptor;
        }
    }
}
