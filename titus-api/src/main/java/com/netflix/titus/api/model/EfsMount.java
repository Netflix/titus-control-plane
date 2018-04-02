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

package com.netflix.titus.api.model;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * EFS mount information
 */
public class EfsMount {

    public enum MountPerm {RO, WO, RW}

    @NotNull(message = "'efsId' is null")
    @Size(min = 1, message = "Emtpy value not allowed")
    private final String efsId;

    @NotNull(message = "'mountPoint' is null")
    @Size(min = 1, message = "Emtpy value not allowed")
    private final String mountPoint;

    @NotNull(message = "'mountPerm' is null")
    private final EfsMount.MountPerm mountPerm;

    private final String efsRelativeMountPoint;

    @JsonCreator
    public EfsMount(@JsonProperty("efsId") String efsId,
                    @JsonProperty("mountPoint") String mountPoint,
                    @JsonProperty("mountPerm") EfsMount.MountPerm mountPerm,
                    @JsonProperty("efsRelativeMountPoint") String efsRelativeMountPoint) {
        this.efsId = efsId;
        this.mountPoint = mountPoint;
        this.mountPerm = mountPerm;
        this.efsRelativeMountPoint = efsRelativeMountPoint;
    }

    public static EfsMount.Builder newBuilder(EfsMount efsMount) {
        return newBuilder()
                .withEfsId(efsMount.getEfsId())
                .withMountPoint(efsMount.getMountPoint())
                .withMountPerm(efsMount.getMountPerm())
                .withEfsRelativeMountPoint(efsMount.getEfsRelativeMountPoint());
    }

    public static EfsMount.Builder newBuilder() {
        return new EfsMount.Builder();
    }

    public String getEfsId() {
        return efsId;
    }

    public String getMountPoint() {
        return mountPoint;
    }

    public EfsMount.MountPerm getMountPerm() {
        return mountPerm;
    }

    public String getEfsRelativeMountPoint() {
        return efsRelativeMountPoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EfsMount efsMount = (EfsMount) o;

        if (efsId != null ? !efsId.equals(efsMount.efsId) : efsMount.efsId != null) {
            return false;
        }
        if (mountPoint != null ? !mountPoint.equals(efsMount.mountPoint) : efsMount.mountPoint != null) {
            return false;
        }
        if (mountPerm != efsMount.mountPerm) {
            return false;
        }
        return efsRelativeMountPoint != null ? efsRelativeMountPoint.equals(efsMount.efsRelativeMountPoint) : efsMount.efsRelativeMountPoint == null;
    }

    @Override
    public int hashCode() {
        int result = efsId != null ? efsId.hashCode() : 0;
        result = 31 * result + (mountPoint != null ? mountPoint.hashCode() : 0);
        result = 31 * result + (mountPerm != null ? mountPerm.hashCode() : 0);
        result = 31 * result + (efsRelativeMountPoint != null ? efsRelativeMountPoint.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EfsMount{" +
                "efsId='" + efsId + '\'' +
                ", mountPoint='" + mountPoint + '\'' +
                ", mountPerm=" + mountPerm +
                ", efsRelativeMountPoint='" + efsRelativeMountPoint + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static final class Builder {
        private String efsId;
        private String mountPoint;
        private EfsMount.MountPerm mountPerm;
        private String efsRelativeMountPoint;

        private Builder() {
        }

        public EfsMount.Builder withEfsId(String efsId) {
            this.efsId = efsId;
            return this;
        }

        public EfsMount.Builder withMountPoint(String mountPoint) {
            this.mountPoint = mountPoint;
            return this;
        }

        public EfsMount.Builder withMountPerm(EfsMount.MountPerm mountPerm) {
            this.mountPerm = mountPerm;
            return this;
        }

        public EfsMount.Builder withEfsRelativeMountPoint(String efsRelativeMountPoint) {
            this.efsRelativeMountPoint = efsRelativeMountPoint;
            return this;
        }

        public EfsMount.Builder but() {
            return newBuilder()
                    .withEfsId(efsId)
                    .withMountPoint(mountPoint)
                    .withMountPerm(mountPerm)
                    .withEfsRelativeMountPoint(efsRelativeMountPoint);
        }

        public EfsMount build() {
            return new EfsMount(efsId, mountPoint, mountPerm, efsRelativeMountPoint);
        }
    }
}
