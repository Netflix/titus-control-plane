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

package io.netflix.titus.api.model;

import javax.validation.constraints.Min;

/**
 * Encapsulates information application or server resources (CPU, memory, disk, etc).
 */
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

    public ResourceDimension(double cpu, long gpu, long memoryMB, long diskMB, long networkMbs) {
        this.cpu = cpu;
        this.gpu = gpu;
        this.memoryMB = memoryMB;
        this.diskMB = diskMB;
        this.networkMbs = networkMbs;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ResourceDimension that = (ResourceDimension) o;

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
        return networkMbs == that.networkMbs;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(cpu);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (gpu ^ (gpu >>> 32));
        result = 31 * result + (int) (memoryMB ^ (memoryMB >>> 32));
        result = 31 * result + (int) (diskMB ^ (diskMB >>> 32));
        result = 31 * result + (int) (networkMbs ^ (networkMbs >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ResourceDimension{" +
                "cpu=" + cpu +
                ", gpu=" + gpu +
                ", memoryMB=" + memoryMB +
                ", diskMB=" + diskMB +
                ", networkMbs=" + networkMbs +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static ResourceDimension empty() {
        return new ResourceDimension(0, 0, 0, 0, 0);
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

        public Builder but() {
            return newBuilder().withCpus(cpus).withMemoryMB(memoryMB).withDiskMB(diskMB).withNetworkMbs(networkMbs);
        }

        public ResourceDimension build() {
            return new ResourceDimension(cpus, gpu, memoryMB, diskMB, networkMbs);
        }
    }
}
