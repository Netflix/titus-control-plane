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

import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class VolumeMount {

    @NotNull
    @Pattern(regexp = "[a-z0-9]([-a-z0-9]*[a-z0-9])?", message = "volume name must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character")
    private final String volumeName;

    @NotNull
    @Pattern(regexp = "^/.*", message = "path must start with a leading slash")
    private final String mountPath;

    private final String mountPropagation;

    private final Boolean readOnly;

    private final String subPath;

    public VolumeMount(String volumeName, String mountPath, String mountPropagation, Boolean readOnly, String subPath) {
        this.volumeName = volumeName;
        this.mountPath = mountPath;
        this.mountPropagation = mountPropagation;
        this.readOnly = readOnly;
        this.subPath = subPath;
    }

    public static Builder newBuilder() {
        return new VolumeMount.Builder();
    }

    public String getMountPropagation() {
        return mountPropagation;
    }

    public Boolean getReadOnly() {
        return readOnly;
    }

    public String getSubPath() {
        return subPath;
    }

    public String getMountPath() {
        return mountPath;
    }

    public String getVolumeName() {
        return volumeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VolumeMount that = (VolumeMount) o;
        return Objects.equals(volumeName, that.volumeName) &&
                Objects.equals(mountPath, that.mountPath) &&
                Objects.equals(mountPropagation, that.mountPropagation) &&
                Objects.equals(readOnly, that.readOnly) &&
                Objects.equals(subPath, that.subPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(volumeName, mountPath, mountPropagation, readOnly, subPath);
    }

    @Override
    public String toString() {
        return "VolumeMount{" +
                "volumeName='" + volumeName + '\'' +
                ", mountPath='" + mountPath + '\'' +
                ", mountPropagation='" + mountPropagation + '\'' +
                ", readOnly=" + readOnly +
                ", subPath='" + subPath + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder();
    }

    public static final class Builder {
        private String volumeName;
        private String mountPath;
        private String mountPropagation;
        private Boolean readOnly;
        private String subPath;

        public Builder withVolumeName(String volumeName) {
            this.volumeName = volumeName;
            return this;
        }

        public Builder withMountPath(String mountPath) {
            this.mountPath = mountPath;
            return this;
        }

        public Builder withMountPropagation(String mountPropagation) {
            this.mountPropagation = mountPropagation;
            return this;
        }

        public Builder withReadOnly(Boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }

        public Builder withSubPath(String subPath) {
            this.subPath = subPath;
            return this;
        }

        public VolumeMount build() {
            return new VolumeMount(volumeName, mountPath, mountPropagation, readOnly, subPath);
        }
    }
}
