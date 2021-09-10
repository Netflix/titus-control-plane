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

import javax.validation.Valid;

public class Volume {

    @Valid
    private final String name;

    private final SharedContainerVolumeSource sharedContainerVolumeSource;

    public Volume(
            String name,
            SharedContainerVolumeSource sharedContainerVolumeSource
    ) {
        this.name = name;
        this.sharedContainerVolumeSource = sharedContainerVolumeSource;
    }

    public String getName() {
        return name;
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Volume volume) {
        return new Builder()
                .withName(volume.getName());
    }

    public SharedContainerVolumeSource getSharedContainerVolumeSource() {
        return this.sharedContainerVolumeSource;
    }

    public static final class Builder {
        private String name;
        private SharedContainerVolumeSource sharedContainerVolumeSource;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withSharedContainerVolumeSource(SharedContainerVolumeSource sharedContainerVolumeSource) {
            this.sharedContainerVolumeSource = sharedContainerVolumeSource;
            return this;
        }

        public Volume build() {
            return new Volume(
                    name,
                    sharedContainerVolumeSource
            );
        }
    }
}
