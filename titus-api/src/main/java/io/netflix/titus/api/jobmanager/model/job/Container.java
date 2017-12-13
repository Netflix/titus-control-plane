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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.validation.Valid;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.SchedulingConstraintSetValidator.SchedulingConstraintSet;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.SchedulingConstraintValidator.SchedulingConstraint;
import io.netflix.titus.common.model.sanitizer.CollectionInvariants;
import io.netflix.titus.common.model.sanitizer.NeverNull;
import io.netflix.titus.common.model.sanitizer.Template;
import io.netflix.titus.common.util.CollectionsExt;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static io.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 */
@NeverNull
@SchedulingConstraintSet
public class Container {

    public static String RESOURCE_CPU = "cpu";
    public static String RESOURCE_GPU = "gpu";
    public static String RESOURCE_MEMORY = "memoryMB";
    public static String RESOURCE_DISK = "diskMB";
    public static String RESOURCE_NETWORK = "networkMbps";

    public static final Set<String> PRIMARY_RESOURCES = asSet(Container.RESOURCE_CPU, Container.RESOURCE_MEMORY, Container.RESOURCE_DISK, Container.RESOURCE_NETWORK);

    @Valid
    private final ContainerResources containerResources;

    @Valid
    private final SecurityProfile securityProfile;

    @Valid
    private final Image image;

    @CollectionInvariants
    private final Map<String, String> attributes;

    @CollectionInvariants
    private final List<String> entryPoint;

    @CollectionInvariants
    private final List<String> command;

    @CollectionInvariants
    private final Map<String, String> env;

    @CollectionInvariants
    @SchedulingConstraint
    private final Map<String, String> softConstraints;

    @CollectionInvariants
    @SchedulingConstraint
    private final Map<String, String> hardConstraints;

    public Container(ContainerResources containerResources,
                     SecurityProfile securityProfile,
                     Image image,
                     Map<String, String> attributes,
                     List<String> entryPoint,
                     List<String> command,
                     Map<String, String> env,
                     Map<String, String> softConstraints,
                     Map<String, String> hardConstraints) {
        this.containerResources = containerResources;
        this.securityProfile = securityProfile;
        this.image = image;
        this.attributes = CollectionsExt.nullableImmutableCopyOf(attributes);
        this.entryPoint = CollectionsExt.nullableImmutableCopyOf(entryPoint);
        this.command = CollectionsExt.nullableImmutableCopyOf(command);
        this.env = CollectionsExt.nullableImmutableCopyOf(env);
        this.softConstraints = CollectionsExt.nullableImmutableCopyOf(softConstraints);
        this.hardConstraints = CollectionsExt.nullableImmutableCopyOf(hardConstraints);
    }

    public ContainerResources getContainerResources() {
        return containerResources;
    }

    public SecurityProfile getSecurityProfile() {
        return securityProfile;
    }

    public Image getImage() {
        return image;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public List<String> getEntryPoint() {
        return entryPoint;
    }

    public List<String> getCommand() {
        return command;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public Map<String, String> getSoftConstraints() {
        return softConstraints;
    }

    public Map<String, String> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Container container = (Container) o;

        if (containerResources != null ? !containerResources.equals(container.containerResources) : container.containerResources != null) {
            return false;
        }
        if (securityProfile != null ? !securityProfile.equals(container.securityProfile) : container.securityProfile != null) {
            return false;
        }
        if (image != null ? !image.equals(container.image) : container.image != null) {
            return false;
        }
        if (attributes != null ? !attributes.equals(container.attributes) : container.attributes != null) {
            return false;
        }
        if (entryPoint != null ? !entryPoint.equals(container.entryPoint) : container.entryPoint != null) {
            return false;
        }
        if (command != null ? !command.equals(container.command) : container.command != null) {
            return false;
        }
        if (env != null ? !env.equals(container.env) : container.env != null) {
            return false;
        }
        if (softConstraints != null ? !softConstraints.equals(container.softConstraints) : container.softConstraints != null) {
            return false;
        }
        return hardConstraints != null ? hardConstraints.equals(container.hardConstraints) : container.hardConstraints == null;
    }

    @Override
    public int hashCode() {
        int result = containerResources != null ? containerResources.hashCode() : 0;
        result = 31 * result + (securityProfile != null ? securityProfile.hashCode() : 0);
        result = 31 * result + (image != null ? image.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (entryPoint != null ? entryPoint.hashCode() : 0);
        result = 31 * result + (command != null ? command.hashCode() : 0);
        result = 31 * result + (env != null ? env.hashCode() : 0);
        result = 31 * result + (softConstraints != null ? softConstraints.hashCode() : 0);
        result = 31 * result + (hardConstraints != null ? hardConstraints.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Container{" +
                "containerResources=" + containerResources +
                ", securityProfile=" + securityProfile +
                ", image=" + image +
                ", attributes=" + attributes +
                ", entryPoint=" + entryPoint +
                ", command=" + command +
                ", env=" + env +
                ", softConstraints=" + softConstraints +
                ", hardConstraints=" + hardConstraints +
                '}';
    }

    public Container but(Function<Container, Object> mapperFun) {
        Object result = mapperFun.apply(this);
        if (result instanceof Container) {
            return (Container) result;
        }
        if (result instanceof Container.Builder) {
            return ((Container.Builder) result).build();
        }
        if (result instanceof ContainerResources) {
            return toBuilder().withContainerResources((ContainerResources) result).build();
        }
        if (result instanceof ContainerResources.Builder) {
            return toBuilder().withContainerResources(((ContainerResources.Builder) result).build()).build();
        }
        if (result instanceof SecurityProfile) {
            return toBuilder().withSecurityProfile((SecurityProfile) result).build();
        }
        if (result instanceof SecurityProfile.Builder) {
            return toBuilder().withSecurityProfile(((SecurityProfile.Builder) result).build()).build();
        }
        if (result instanceof Image) {
            return toBuilder().withImage((Image) result).build();
        }
        if (result instanceof Image.Builder) {
            return toBuilder().withImage(((Image.Builder) result).build()).build();
        }
        throw new IllegalArgumentException("Invalid result type " + result.getClass());
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Container container) {
        return new Builder()
                .withContainerResources(container.getContainerResources())
                .withSecurityProfile(container.getSecurityProfile())
                .withImage(container.getImage())
                .withAttributes(container.getAttributes())
                .withEntryPoint(container.getEntryPoint())
                .withCommand(container.getCommand())
                .withEnv(container.getEnv())
                .withSoftConstraints(container.getSoftConstraints())
                .withHardConstraints(container.getHardConstraints());
    }

    public static final class Builder {
        private ContainerResources containerResources;
        private SecurityProfile securityProfile;
        private Image image;
        private Map<String, String> attributes;
        private List<String> entryPoint;
        private List<String> command;
        private Map<String, String> env;
        private Map<String, String> softConstraints;
        private Map<String, String> hardConstraints;

        private Builder() {
        }

        public Builder withContainerResources(ContainerResources containerResources) {
            this.containerResources = containerResources;
            return this;
        }

        public Builder withSecurityProfile(SecurityProfile securityProfile) {
            this.securityProfile = securityProfile;
            return this;
        }

        public Builder withImage(Image image) {
            this.image = image;
            return this;
        }

        public Builder withAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder withEntryPoint(List<String> entryPoint) {
            this.entryPoint = entryPoint;
            return this;
        }

        public Builder withCommand(List<String> command) {
            this.command = command;
            return this;
        }

        public Builder withEnv(Map<String, String> env) {
            this.env = env;
            return this;
        }

        public Builder withSoftConstraints(Map<String, String> softConstraints) {
            this.softConstraints = softConstraints;
            return this;
        }

        public Builder withHardConstraints(Map<String, String> hardConstraints) {
            this.hardConstraints = hardConstraints;
            return this;
        }

        public Container build() {
            Preconditions.checkNotNull(containerResources, "ContainerResources not defined");
            Preconditions.checkNotNull(image, "Image not defined");

            Container container = new Container(
                    containerResources,
                    securityProfile == null ? SecurityProfile.empty() : securityProfile,
                    image,
                    nonNull(attributes),
                    entryPoint,
                    command,
                    nonNull(env),
                    nonNull(softConstraints),
                    nonNull(hardConstraints)
            );
            return container;
        }
    }
}
