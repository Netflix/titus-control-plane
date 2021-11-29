/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job.volume;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class SharedContainerVolumeSource extends VolumeSource {

    @NotNull
    @Pattern(regexp = "[a-z0-9]([-a-z0-9]*[a-z0-9])?", message = "source container must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character")
    private final String sourceContainer;

    @NotNull
    @Pattern(regexp = "^/.*", message = "sourcePath must start with a leading slash")
    private final String sourcePath;

    public SharedContainerVolumeSource(
            String sourceContainer,
            String sourcePath
    ) {
        this.sourceContainer = sourceContainer;
        this.sourcePath = sourcePath;
    }

    public String getSourceContainer() {
        return sourceContainer;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    @Override
    public String toString() {
        return "VolumeSource{" +
                "sourceContainer='" + sourceContainer + '\'' +
                ", sourcePath='" + sourcePath + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return sourceContainer.hashCode() + sourcePath.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SharedContainerVolumeSource that = (SharedContainerVolumeSource) o;
        if (!this.getSourceContainer().equals(that.getSourceContainer())) {
            return false;
        }
        return this.getSourcePath().equals(that.getSourcePath());
    }

    public static Builder newBuilder() {
        return new SharedContainerVolumeSource.Builder();
    }

    public static Builder newBuilder(SharedContainerVolumeSource sharedContainerVolume) {
        return new SharedContainerVolumeSource.Builder()
                .withSourceContainer(sharedContainerVolume.sourceContainer)
                .withSourcePath(sharedContainerVolume.sourcePath);
    }

    public static final class Builder<E extends VolumeSource> {
        private String sourceContainer;
        private String sourcePath;

        public Builder<E> withSourceContainer(String sourceContainer) {
            this.sourceContainer = sourceContainer;
            return this;
        }

        public Builder<E> withSourcePath(String sourcePath) {
            this.sourcePath = sourcePath;
            return this;
        }

        public SharedContainerVolumeSource build() {
            return new SharedContainerVolumeSource(
                    sourceContainer,
                    sourcePath
            );
        }
    }

}
