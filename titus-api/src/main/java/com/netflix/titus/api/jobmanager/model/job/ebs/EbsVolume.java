/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.api.jobmanager.model.job.ebs;

import java.util.Objects;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.google.common.base.Preconditions;

public class EbsVolume {

    public enum MountPerm {RO, RW}

    public EbsVolume(String volumeId, String volumeAvailabilityZone, int volumeCapacityGB, String mountPath, MountPerm mountPerm, String fsType) {
        this.volumeId = volumeId;
        this.volumeAvailabilityZone = volumeAvailabilityZone;
        this.volumeCapacityGB = volumeCapacityGB;
        this.mountPath = mountPath;
        this.mountPermissions = mountPerm;
        this.fsType = fsType;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    @NotNull
    @Size(min = 1, max = 512, message = "EBS volume ID cannot be empty or greater than 512 bytes")
    private final String volumeId;

    @Size(min = 1, max = 512, message = "EBS volume AZ cannot be empty or greater than 512 bytes")
    private final String volumeAvailabilityZone;

    private final int volumeCapacityGB;

    @NotNull
    @Size(min = 1, max = 1024, message = "EBS volume mount path cannot be empty or greater than 1024 bytes")
    private final String mountPath;

    @NotNull(message = "'mountPermissions' is null")
    private final MountPerm mountPermissions;

    @NotNull
    @Size(min = 1, max = 512, message = "EBS volume FS type cannot be empty or greater than 512 bytes")
    private final String fsType;

    public String getVolumeId() {
        return volumeId;
    }

    public String getVolumeAvailabilityZone() {
        return volumeAvailabilityZone;
    }

    public int getVolumeCapacityGB() {
        return volumeCapacityGB;
    }

    public String getMountPath() {
        return mountPath;
    }

    public MountPerm getMountPermissions() {
        return mountPermissions;
    }

    public String getFsType() {
        return fsType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EbsVolume ebsVolume = (EbsVolume) o;
        return volumeCapacityGB == ebsVolume.volumeCapacityGB &&
                volumeId.equals(ebsVolume.volumeId) &&
                Objects.equals(volumeAvailabilityZone, ebsVolume.volumeAvailabilityZone) &&
                mountPath.equals(ebsVolume.mountPath) &&
                mountPermissions == ebsVolume.mountPermissions &&
                fsType.equals(ebsVolume.fsType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(volumeId, volumeAvailabilityZone, volumeCapacityGB, mountPath, mountPermissions, fsType);
    }

    @Override
    public String toString() {
        return "EbsVolume{" +
                "volumeId='" + volumeId + '\'' +
                ", volumeAvailabilityZone='" + volumeAvailabilityZone + '\'' +
                ", volumeCapacityGB=" + volumeCapacityGB +
                ", mountPath='" + mountPath + '\'' +
                ", mountPermissions=" + mountPermissions +
                ", fsType='" + fsType + '\'' +
                '}';
    }

    public static final class Builder {
        private String volumeId;
        private String volumeAvailabilityZone;
        private int volumeCapacityGB;
        private String mountPath;
        private MountPerm mountPermissions;
        private String fsType;

        private Builder() {
        }

        private Builder(EbsVolume ebsVolume) {
            this.volumeId = ebsVolume.getVolumeId();
            this.volumeAvailabilityZone = ebsVolume.getVolumeAvailabilityZone();
            this.volumeCapacityGB = ebsVolume.getVolumeCapacityGB();
            this.mountPath = ebsVolume.getMountPath();
            this.mountPermissions = ebsVolume.getMountPermissions();
            this.fsType = ebsVolume.getFsType();
        }

        public Builder withVolumeId(String val) {
            volumeId = val;
            return this;
        }

        public Builder withVolumeAvailabilityZone(String val) {
            volumeAvailabilityZone = val;
            return this;
        }

        public Builder withVolumeCapacityGB(int val) {
            volumeCapacityGB = val;
            return this;
        }

        public Builder withMountPath(String val) {
            mountPath = val;
            return this;
        }

        public Builder withMountPermissions(MountPerm val) {
            mountPermissions = val;
            return this;
        }

        public Builder withFsType(String val) {
            fsType = val;
            return this;
        }

        public EbsVolume build() {
            Preconditions.checkNotNull(volumeId, "Volume ID is null");
            Preconditions.checkNotNull(mountPath, "Mount path is null");
            Preconditions.checkNotNull(mountPermissions, "Mount permission is null");
            Preconditions.checkNotNull(fsType, "File system type is null");

            // Volume AZ and capacity may be set after object creation during object sanitization
            return new EbsVolume(volumeId, volumeAvailabilityZone, volumeCapacityGB, mountPath, mountPermissions, fsType);
        }
    }
}
