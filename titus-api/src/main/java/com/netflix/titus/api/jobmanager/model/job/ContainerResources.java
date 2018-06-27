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
import javax.validation.Valid;
import javax.validation.constraints.Min;

import com.netflix.titus.api.jobmanager.model.job.sanitizer.EfsMountsSanitizer;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.model.sanitizer.FieldInvariant;
import com.netflix.titus.common.model.sanitizer.FieldSanitizer;
import com.netflix.titus.common.util.CollectionsExt;

import static com.netflix.titus.common.util.CollectionsExt.nonNull;

/**
 */
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

    public ContainerResources(double cpu, int gpu, int memoryMB, int diskMB, int networkMbps, List<EfsMount> efsMounts, boolean allocateIP) {
        this.cpu = cpu;
        this.gpu = gpu;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.networkMbps = networkMbps;
        this.efsMounts = CollectionsExt.nullableImmutableCopyOf(efsMounts);
        this.allocateIP = allocateIP;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ContainerResources that = (ContainerResources) o;

        if (Double.compare(that.cpu, cpu) != 0) {
            return false;
        }
        if (gpu != that.gpu) {
            return false;
        }
        if (memoryMB != that.memoryMB) {
            return false;
        }
        if (diskMB != that.diskMB) {
            return false;
        }
        if (networkMbps != that.networkMbps) {
            return false;
        }
        if (allocateIP != that.allocateIP) {
            return false;
        }
        return efsMounts != null ? efsMounts.equals(that.efsMounts) : that.efsMounts == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(cpu);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + gpu;
        result = 31 * result + memoryMB;
        result = 31 * result + diskMB;
        result = 31 * result + networkMbps;
        result = 31 * result + (efsMounts != null ? efsMounts.hashCode() : 0);
        result = 31 * result + (allocateIP ? 1 : 0);
        return result;
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
                .withEfsMounts(containerResources.getEfsMounts());
    }

    public static final class Builder {
        private double cpu;
        private int gpu;
        private int memoryMB;
        private int diskMB;
        private int networkMbps;
        private List<EfsMount> efsMounts;
        private boolean allocateIP;

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

        public Builder but() {
            return newBuilder().withCpu(cpu).withGpu(gpu).withMemoryMB(memoryMB).withDiskMB(diskMB).withNetworkMbps(networkMbps).withEfsMounts(efsMounts);
        }

        public ContainerResources build() {
            ContainerResources containerResources = new ContainerResources(cpu, gpu, memoryMB, diskMB, networkMbps, nonNull(efsMounts), allocateIP);
            return containerResources;
        }
    }
}
