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

package io.netflix.titus.api.endpoint.v2.rest.representation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * EFS mount information
 */
public class EfsMountRepresentation {
    public enum MountPerm {RO, WO, RW}

    private final String efsId;
    private final String mountPoint;
    private final MountPerm mountPerm;
    private final String efsRelativeMountPoint;

    @JsonCreator
    public EfsMountRepresentation(@JsonProperty("efsId") String efsId,
                                  @JsonProperty("mountPoint") String mountPoint,
                                  @JsonProperty("mountPerm") MountPerm mountPerm,
                                  @JsonProperty("efsRelativeMountPoint") String efsRelativeMountPoint) {
        this.efsId = efsId;
        this.mountPoint = mountPoint;
        this.mountPerm = mountPerm;
        this.efsRelativeMountPoint = efsRelativeMountPoint;
    }

    public static Builder newBuilder(EfsMountRepresentation efsMount) {
        return newBuilder()
                .withEfsId(efsMount.getEfsId())
                .withMountPoint(efsMount.getMountPoint())
                .withMountPerm(efsMount.getMountPerm())
                .withEfsRelativeMountPoint(efsMount.getEfsRelativeMountPoint());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getEfsId() {
        return efsId;
    }

    public String getMountPoint() {
        return mountPoint;
    }

    public MountPerm getMountPerm() {
        return mountPerm;
    }

    public String getEfsRelativeMountPoint() {
        return efsRelativeMountPoint;
    }

    public static final class Builder {
        private String efsId;
        private String mountPoint;
        private MountPerm mountPerm;
        private String efsRelativeMountPoint;

        private Builder() {
        }

        public Builder withEfsId(String efsId) {
            this.efsId = efsId;
            return this;
        }

        public Builder withMountPoint(String mountPoint) {
            this.mountPoint = mountPoint;
            return this;
        }

        public Builder withMountPerm(MountPerm mountPerm) {
            this.mountPerm = mountPerm;
            return this;
        }

        public Builder withEfsRelativeMountPoint(String efsRelativeMountPoint) {
            this.efsRelativeMountPoint = efsRelativeMountPoint;
            return this;
        }

        public Builder but() {
            return newBuilder().withEfsId(efsId).withMountPoint(mountPoint).withMountPerm(mountPerm);
        }

        public EfsMountRepresentation build() {
            EfsMountRepresentation efsMount = new EfsMountRepresentation(efsId, mountPoint, mountPerm, efsRelativeMountPoint);
            return efsMount;
        }
    }
}
