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

import javax.validation.Valid;

public class Volume {
    @Valid
    private String name;

    private VolumeSource volumeSource;

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

    public Builder toBuilder() {
        return newBuilder();
    }

    public static Builder newBuilder() {
        return new Volume.Builder();
    }

    public static class Builder {

        private String name;

        private VolumeSource volumeSource;

        public Builder(String name) {
            this.name = name;
        }

        public Builder() {
        }

        public Volume build() {
            return new Volume(name, volumeSource);
        }

        public Builder withName(String name) {
            return new Volume(name, volumeSource).toBuilder();
        }

        public Builder withVolumeSource(VolumeSource volumeSource) {
            return new Volume(name, volumeSource).toBuilder();
        }
    }
}
