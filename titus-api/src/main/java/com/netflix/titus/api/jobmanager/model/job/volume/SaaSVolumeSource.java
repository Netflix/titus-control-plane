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

import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class SaaSVolumeSource extends VolumeSource {

    @NotNull
    @Pattern(regexp = "[a-z0-9]([-a-z0-9]*[a-z0-9])?", message = "SaaS Volume ID must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character")
    private final String saaSVolumeID;

    public SaaSVolumeSource(String saaSVolumeID) {
        this.saaSVolumeID = saaSVolumeID;
    }

    public static SaaSVolumeSource.Builder newBuilder() {
        return new SaaSVolumeSource.Builder();
    }

    public static SaaSVolumeSource.Builder newBuilder(SaaSVolumeSource saaSVolumeSource) {
        return new SaaSVolumeSource.Builder()
                .withSaaSVolumeID(saaSVolumeSource.saaSVolumeID);
    }

    public String getSaaSVolumeID() {
        return saaSVolumeID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SaaSVolumeSource that = (SaaSVolumeSource) o;
        return Objects.equals(saaSVolumeID, that.saaSVolumeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(saaSVolumeID);
    }

    @Override
    public String toString() {
        return "SaaSVolumeSource{" +
                "SaaSVolumeID='" + saaSVolumeID + '\'' +
                '}';
    }

    public static final class Builder<E extends VolumeSource> {
        private String saaSVolumeID;

        public SaaSVolumeSource.Builder<E> withSaaSVolumeID(String saaSVolumeID) {
            this.saaSVolumeID = saaSVolumeID;
            return this;
        }

        public SaaSVolumeSource build() {
            return new SaaSVolumeSource(
                    saaSVolumeID
            );
        }
    }
}
