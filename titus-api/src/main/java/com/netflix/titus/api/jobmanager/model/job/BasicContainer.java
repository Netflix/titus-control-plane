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
import java.util.Map;
import javax.validation.Valid;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.common.model.sanitizer.CollectionInvariants;
import com.netflix.titus.common.model.sanitizer.FieldInvariant;
import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;

public class BasicContainer {

    @Valid
    private final String name;

    @Valid
    private final Image image;

    @CollectionInvariants
    @FieldInvariant(
            value = "@asserts.isEntryPointNotTooLarge(value)",
            message = "Entry point size exceeds the limit " + JobAssertions.MAX_ENTRY_POINT_SIZE_SIZE_KB + "KB"
    )
    private final List<String> entryPoint;

    @CollectionInvariants
    private final List<String> command;

    @CollectionInvariants(allowEmptyKeys = false)
    @FieldInvariant(
            value = "@asserts.areEnvironmentVariablesNotTooLarge(value)",
            message = "Container environment variables size exceeds the limit"
    )
    private final Map<String, String> env;

    private final List<VolumeMount> volumeMounts;

    public BasicContainer(
            String name,
            Image image,
            List<String> entryPoint,
            List<String> command,
            Map<String, String> env,
            List<VolumeMount> volumeMounts
    ) {
        this.name = name;
        this.image = image;
        this.entryPoint = CollectionsExt.nullableImmutableCopyOf(entryPoint);
        this.command = CollectionsExt.nullableImmutableCopyOf(command);
        this.env = CollectionsExt.nullableImmutableCopyOf(env);
        this.volumeMounts = CollectionsExt.nullableImmutableCopyOf(volumeMounts);
    }

    public String getName() {
        return name;
    }

    public Image getImage() {
        return image;
    }

    public List<String> getEntryPoint() {
        if (entryPoint == null) {
            return Collections.emptyList();
        }
        return entryPoint;
    }

    public List<String> getCommand() {
        if (command == null) {
            return Collections.emptyList();
        }
        return command;
    }

    public Map<String, String> getEnv() {
        if (env == null) {
            return Collections.emptyMap();
        }
        return env;
    }

    public List<VolumeMount> getVolumeMounts() {
        return volumeMounts;
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(BasicContainer basicContainer) {
        return new Builder()
                .withName(basicContainer.getName())
                .withImage(basicContainer.getImage())
                .withEntryPoint(basicContainer.getEntryPoint())
                .withCommand(basicContainer.getCommand())
                .withEnv(basicContainer.getEnv())
                .withVolumeMounts(basicContainer.getVolumeMounts());
    }

    public static final class Builder {
        private String name;
        private Image image;
        private List<String> entryPoint;
        private List<String> command;
        private Map<String, String> env;
        private List<VolumeMount> volumeMounts;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withImage(Image image) {
            this.image = image;
            return this;
        }

        public Builder withEntryPoint(List<String> entryPoint) {
            if (entryPoint == null) {
                entryPoint = Collections.emptyList();
            }
            this.entryPoint = entryPoint;
            return this;
        }

        public Builder withCommand(List<String> command) {
            if (command == null) {
                command = Collections.emptyList();
            }
            this.command = command;
            return this;
        }

        public Builder withEnv(Map<String, String> env) {
            this.env = env;
            return this;
        }

        public Builder withVolumeMounts(List<VolumeMount> volumeMounts) {
            this.volumeMounts = volumeMounts;
            return this;
        }

        public BasicContainer build() {
            Preconditions.checkNotNull(image, "Image not defined");
            return new BasicContainer(
                    name,
                    image,
                    entryPoint,
                    command,
                    nonNull(env),
                    nonNull(volumeMounts)
            );
        }
    }
}
