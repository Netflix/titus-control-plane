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

import java.util.Objects;
import javax.validation.constraints.Min;

/**
 * Encapsulates information application or server resources (CPU, memory, disk, etc).
 */
// TODO: this model is also used to represent resource asks (reservations) and utilization, which have different
//  requirements (e.g.: not everything that is being utilized was reserved)
public class ResourceDimension {

    @Min(value = 0, message = "'cpu' must be >= 0, but is #{#root}")
    private final double cpu;

    @Min(value = 0, message = "'gpus' must be >= 0, but is #{#root}")
    private final long gpu;

    @Min(value = 0, message = "'memoryMB' must be >= 0, but is #{#root}")
    private final long memoryMB;

    @Min(value = 0, message = "'diskMB' must be >= 0, but is #{#root}")
    private final long diskMB;

    @Min(value = 0, message = "'networkMbs' must be >= 0, but is #{#root}")
    private final long networkMbs;

    @Min(value = 0, message = "'opportunisticCpu' must be >= 0, but is #{#root}")
    private final long opportunisticCpu;

    public ResourceDimension(double cpu, long gpu, long memoryMB, long diskMB, long networkMbs, long opportunisticCpu) {
        this.cpu = cpu;
        this.gpu = gpu;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.networkMbs = networkMbs;
        this.opportunisticCpu = opportunisticCpu;
    }

    public double getCpu() {
        return cpu;
    }

    public long getGpu() {
        return gpu;
    }

    public long getMemoryMB() {
        return memoryMB;
    }

    public long getDiskMB() {
        return diskMB;
    }

    public long getNetworkMbs() {
        return networkMbs;
    }

    public long getOpportunisticCpu() {
        return opportunisticCpu;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceDimension that = (ResourceDimension) o;
        return Double.compare(that.cpu, cpu) == 0 &&
                gpu == that.gpu &&
                memoryMB == that.memoryMB &&
                diskMB == that.diskMB &&
                networkMbs == that.networkMbs &&
                opportunisticCpu == that.opportunisticCpu;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpu, gpu, memoryMB, diskMB, networkMbs, opportunisticCpu);
    }

    @Override
    public String toString() {
        return "ResourceDimension{" +
                "cpu=" + cpu +
                ", gpu=" + gpu +
                ", memoryMB=" + memoryMB +
                ", diskMB=" + diskMB +
                ", networkMbs=" + networkMbs +
                ", opportunisticCpu=" + opportunisticCpu +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static ResourceDimension empty() {
        return new ResourceDimension(0, 0, 0, 0, 0, 0);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ResourceDimension source) {
        return new Builder()
                .withCpus(source.getCpu())
                .withMemoryMB(source.getMemoryMB())
                .withDiskMB(source.getDiskMB())
                .withNetworkMbs(source.getNetworkMbs());
    }

    public static final class Builder {
        private double cpus;
        private long gpu;
        private long memoryMB;
        private long diskMB;
        private long networkMbs;
        private long opportunisticCpus;

        private Builder() {
        }

        public Builder withCpus(double cpus) {
            this.cpus = cpus;
            return this;
        }

        public Builder withGpu(long gpu) {
            this.gpu = gpu;
            return this;
        }

        public Builder withMemoryMB(long memoryMB) {
            this.memoryMB = memoryMB;
            return this;
        }

        public Builder withDiskMB(long diskMB) {
            this.diskMB = diskMB;
            return this;
        }

        public Builder withNetworkMbs(long networkMbs) {
            this.networkMbs = networkMbs;
            return this;
        }

        public Builder withOpportunisticCpus(long cpus) {
            this.opportunisticCpus = cpus;
            return this;
        }

        public Builder but() {
            return newBuilder().withCpus(cpus).withMemoryMB(memoryMB).withDiskMB(diskMB).withNetworkMbs(networkMbs)
                    .withOpportunisticCpus(opportunisticCpus);
        }

        public ResourceDimension build() {
            return new ResourceDimension(cpus, gpu, memoryMB, diskMB, networkMbs, opportunisticCpus);
        }
    }
}
