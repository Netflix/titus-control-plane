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

import java.util.List;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.Min;

import com.netflix.titus.api.jobmanager.model.job.sanitizer.EfsMountsSanitizer;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.model.sanitizer.ClassInvariant;
import com.netflix.titus.common.model.sanitizer.FieldInvariant;
import com.netflix.titus.common.model.sanitizer.FieldSanitizer;
import com.netflix.titus.common.util.CollectionsExt;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 */
@ClassInvariant(condition = "shmMB <= memoryMB", message = "'shmMB' (#{shmMB}) must be <= 'memoryMB' (#{memoryMB})")
public class ContainerResources {

    @FieldSanitizer(adjuster = "T(java.lang.Math).max(@constraints.getCpuMin(), value)")
    @FieldInvariant(value = "value > 0", message = "'cpu'(#{value}) must be > 0")
    private final double cpu;

    @Min(value = 0, message = "'gpu'(#{#root}) must be >= 0")
    private final int gpu;

    @FieldSanitizer(adjuster = "T(java.lang.Math).max(@constraints.getMemoryMegabytesMin(), value)")
    @Min(value = 1, message = "'memoryMB'(#{#root}) must be > 0")
    private final int memoryMB;

    @FieldSanitizer(adjuster = "T(java.lang.Math).max(@constraints.getDiskMegabytesMin(), value)")
    @Min(value = 1, message = "'diskMB'(#{#root}) must be > 0")
    private final int diskMB;

    @FieldSanitizer(adjuster = "T(java.lang.Math).max(@constraints.getNetworkMbpsMin(), value)")
    @Min(value = 1, message = "'networkMbps'(#{#root}) must be > 0")
    private final int networkMbps;

    @FieldSanitizer(sanitizer = EfsMountsSanitizer.class)
    @Valid
    private final List<EfsMount> efsMounts;

    private final boolean allocateIP;

    // If provided value is 0, rewrite to a common default
    @FieldSanitizer(adjuster = "value == 0 ? @constraints.getShmMegabytesDefault() : value")
    @Min(value = 0, message = "'shmMB'(#{#root}) must be >= 0")
    private final int shmMB;

    public ContainerResources(double cpu,
                              int gpu,
                              int memoryMB,
                              int diskMB,
                              int networkMbps,
                              List<EfsMount> efsMounts,
                              boolean allocateIP,
                              int shmMB) {
        this.cpu = cpu;
        this.gpu = gpu;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.networkMbps = networkMbps;
        this.efsMounts = CollectionsExt.nullableImmutableCopyOf(efsMounts);
        this.allocateIP = allocateIP;
        this.shmMB = shmMB;
    }
    public double getCpu() {
        return cpu;
    }

    public int getGpu() {
        return gpu;
    }

    public int getMemoryMB() {
        return memoryMB;
    }

    public int getDiskMB() {
        return diskMB;
    }

    public int getNetworkMbps() {
        return networkMbps;
    }

    public List<EfsMount> getEfsMounts() {
        return efsMounts;
    }

    public boolean isAllocateIP() {
        return allocateIP;
    }

    public int getShmMB() {
        return shmMB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerResources that = (ContainerResources) o;
        return Double.compare(that.cpu, cpu) == 0 &&
                gpu == that.gpu &&
                memoryMB == that.memoryMB &&
                diskMB == that.diskMB &&
                networkMbps == that.networkMbps &&
                allocateIP == that.allocateIP &&
                shmMB == that.shmMB &&
                Objects.equals(efsMounts, that.efsMounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpu, gpu, memoryMB, diskMB, networkMbps, efsMounts, allocateIP, shmMB);
    }

    @Override
    public String toString() {
        return "ContainerResources{" +
                "cpu=" + cpu +
                ", gpu=" + gpu +
                ", memoryMB=" + memoryMB +
                ", diskMB=" + diskMB +
                ", networkMbps=" + networkMbps +
                ", efsMounts=" + efsMounts +
                ", allocateIP=" + allocateIP +
                ", shmMB=" + shmMB +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ContainerResources containerResources) {
        return new Builder()
                .withCpu(containerResources.getCpu())
                .withGpu(containerResources.getGpu())
                .withMemoryMB(containerResources.getMemoryMB())
                .withDiskMB(containerResources.getDiskMB())
                .withNetworkMbps(containerResources.getNetworkMbps())
                .withAllocateIP(containerResources.isAllocateIP())
                .withEfsMounts(containerResources.getEfsMounts())
                .withShmMB(containerResources.getShmMB());
    }

    public static final class Builder {
        private double cpu;
        private int gpu;
        private int memoryMB;
        private int diskMB;
        private int networkMbps;
        private List<EfsMount> efsMounts;
        private boolean allocateIP;
        private int shmMB;

        private Builder() {
        }

        public Builder withCpu(double cpu) {
            this.cpu = cpu;
            return this;
        }

        public Builder withGpu(int gpu) {
            this.gpu = gpu;
            return this;
        }

        public Builder withMemoryMB(int memoryMB) {
            this.memoryMB = memoryMB;
            return this;
        }

        public Builder withDiskMB(int diskMB) {
            this.diskMB = diskMB;
            return this;
        }

        public Builder withNetworkMbps(int networkMbps) {
            this.networkMbps = networkMbps;
            return this;
        }

        public Builder withEfsMounts(List<EfsMount> efsMounts) {
            this.efsMounts = efsMounts;
            return this;
        }

        public Builder withAllocateIP(boolean allocateIP) {
            this.allocateIP = allocateIP;
            return this;
        }

        public Builder withShmMB(int shmMB) {
            this.shmMB = shmMB;
            return this;
        }

        public Builder but() {
            return newBuilder()
                    .withCpu(cpu)
                    .withGpu(gpu)
                    .withMemoryMB(memoryMB)
                    .withDiskMB(diskMB)
                    .withNetworkMbps(networkMbps)
                    .withEfsMounts(efsMounts)
                    .withShmMB(shmMB);
        }

        public ContainerResources build() {
            ContainerResources containerResources = new ContainerResources(cpu, gpu, memoryMB, diskMB, networkMbps, nonNull(efsMounts), allocateIP, shmMB);
            return containerResources;
        }
    }
}
