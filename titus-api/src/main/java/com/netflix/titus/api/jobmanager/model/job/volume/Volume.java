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

package com.netflix.titus.api.jobmanager.model.job.volume;

import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.Pattern;

public class Volume {
    @Valid
    @Pattern(regexp = "[a-z0-9]([-a-z0-9]*[a-z0-9])?", message = "volume name must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character")
    private final String name;

    @Valid
    private final VolumeSource volumeSource;

    public Volume(String name, VolumeSource volumeSource) {
        this.name = name;
        this.volumeSource = volumeSource;
    }

    public String getName() {
        return name;
    }

    public VolumeSource getVolumeSource() {
        return volumeSource;
    }

    @Override
    public String toString() {
        return "Volume{" +
                "name='" + name + '\'' +
                ", volumeSource='" + volumeSource + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, volumeSource.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Volume that = (Volume) o;
        if (!this.getName().equals(that.getName())) {
            return false;
        }
        return this.getVolumeSource().equals(that.getVolumeSource());
    }

    public Builder toBuilder() {
        return newBuilder();
    }

    public static Builder newBuilder() {
        return new Volume.Builder();
    }

    public static final class Builder {
        private String name;
        private VolumeSource volumeSource;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withVolumeSource(VolumeSource volumeSource) {
            this.volumeSource = volumeSource;
            return this;
        }

        public Volume build() {
            return new Volume(name, volumeSource);
        }
    }
}
