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

package com.netflix.titus.master.model;

import java.util.Collection;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.common.util.tuple.Pair;

import static com.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

/**
 * {@link ResourceDimension} related functions.
 */
public class ResourceDimensions {

    /**
     * Returns a new {@link ResourceDimension} instance with resources being some of all the provided.
     */
    public static ResourceDimension add(ResourceDimension... parts) {
        if (isNullOrEmpty(parts)) {
            return ResourceDimension.empty();
        }
        double cpuSum = 0;
        long memorySum = 0;
        long diskSum = 0;
        long networkSum = 0;
        long gpuSum = 0;
        long opportunisticCpuSum = 0;
        for (ResourceDimension part : parts) {
            cpuSum += part.getCpu();
            memorySum += part.getMemoryMB();
            diskSum += part.getDiskMB();
            networkSum += part.getNetworkMbs();
            gpuSum += part.getGpu();
            opportunisticCpuSum += part.getOpportunisticCpu();
        }
        return ResourceDimension.newBuilder()
                .withCpus(cpuSum)
                .withMemoryMB(memorySum)
                .withDiskMB(diskSum)
                .withNetworkMbs(networkSum)
                .withGpu(gpuSum)
                .withOpportunisticCpus(opportunisticCpuSum)
                .build();
    }

    /**
     * Returns a new {@link ResourceDimension} instance with resources being some of all the provided.
     */
    public static ResourceDimension add(Collection<ResourceDimension> parts) {
        return isNullOrEmpty(parts) ? ResourceDimension.empty() : add(parts.toArray(new ResourceDimension[parts.size()]));
    }

    /**
     * Subtract resources. If right side resource value (CPU, memory, etc) is greater than the left side, set it to 0.
     *
     * @throws IllegalArgumentException if left side resources are smaller then the right side
     */
    public static ResourceDimension subtractPositive(ResourceDimension left, ResourceDimension right) {
        return ResourceDimension.newBuilder()
                .withCpus(Math.max(0, left.getCpu() - right.getCpu()))
                .withMemoryMB(Math.max(0, left.getMemoryMB() - right.getMemoryMB()))
                .withDiskMB(Math.max(0, left.getDiskMB() - right.getDiskMB()))
                .withNetworkMbs(Math.max(0, left.getNetworkMbs() - right.getNetworkMbs()))
                .withGpu(Math.max(0, left.getGpu() - right.getGpu()))
                .withOpportunisticCpus(Math.max(0, left.getOpportunisticCpu() - right.getOpportunisticCpu()))
                .build();
    }

    /**
     * Multiple all resources of the {@link ResourceDimension} instance by the given number.
     */
    public static ResourceDimension multiply(ResourceDimension base, double multiplier) {
        return ResourceDimension.newBuilder()
                .withCpus(base.getCpu() * multiplier)
                .withMemoryMB((long) Math.ceil(base.getMemoryMB() * multiplier))
                .withDiskMB((long) Math.ceil(base.getDiskMB() * multiplier))
                .withNetworkMbs((long) Math.ceil(base.getNetworkMbs() * multiplier))
                .withGpu((long) Math.ceil(base.getGpu() * multiplier))
                .withOpportunisticCpus((long) Math.ceil(base.getOpportunisticCpu() * multiplier))
                .build();
    }

    public static Pair<Long, ResourceDimension> divide(ResourceDimension left, ResourceDimension right) {
        double multiplier = 0;

        if (left.getCpu() != 0) {
            Preconditions.checkArgument(right.getCpu() != 0, "CPU: division by 0");
            multiplier = left.getCpu() / right.getCpu();
        }
        if (left.getMemoryMB() != 0) {
            Preconditions.checkArgument(right.getMemoryMB() != 0, "MemoryMB: division by 0");
            multiplier = Math.max(multiplier, left.getMemoryMB() / right.getMemoryMB());
        }
        if (left.getDiskMB() != 0) {
            Preconditions.checkArgument(right.getDiskMB() != 0, "DiskMB: division by 0");
            multiplier = Math.max(multiplier, left.getDiskMB() / right.getDiskMB());
        }
        if (left.getNetworkMbs() != 0) {
            Preconditions.checkArgument(right.getNetworkMbs() != 0, "NetworkMbs: division by 0");
            multiplier = Math.max(multiplier, left.getNetworkMbs() / right.getNetworkMbs());
        }
        if (left.getGpu() != 0) {
            Preconditions.checkArgument(right.getGpu() != 0, "GPU: division by 0");
            multiplier = Math.max(multiplier, left.getGpu() / right.getGpu());
        }
        if (left.getOpportunisticCpu() != 0) {
            Preconditions.checkArgument(right.getOpportunisticCpu() != 0, "Opportunistic CPU: division by 0");
            multiplier = Math.max(multiplier, left.getOpportunisticCpu() / right.getOpportunisticCpu());
        }

        if (multiplier == 0) { // left is empty
            return Pair.of(0L, ResourceDimension.empty());
        }
        if (multiplier < 1) { // left < right
            return Pair.of(0L, left);
        }
        if (multiplier == 1) {
            return Pair.of(1L, ResourceDimension.empty());
        }

        long full = (long) multiplier;

        return Pair.of(full,
                ResourceDimension.newBuilder()
                        .withCpus(Math.max(0, left.getCpu() - right.getCpu() * full))
                        .withMemoryMB(Math.max(0, left.getMemoryMB() - right.getMemoryMB() * full))
                        .withDiskMB(Math.max(0, left.getDiskMB() - right.getDiskMB() * full))
                        .withNetworkMbs(Math.max(0, left.getNetworkMbs() - right.getNetworkMbs() * full))
                        .withGpu(Math.max(0, left.getGpu() - right.getGpu() * full))
                        .withOpportunisticCpus(Math.max(0, left.getOpportunisticCpu() - right.getOpportunisticCpu() * full))
                        .build()
        );
    }

    public static long divideAndRoundUp(ResourceDimension left, ResourceDimension right) {
        Pair<Long, ResourceDimension> result = divide(left, right);
        return result.getRight().equals(ResourceDimension.empty()) ? result.getLeft() : result.getLeft() + 1;
    }

    /**
     * Align source {@link ResourceDimension} resources, to match resource ratios (cpu and memory) from the reference
     * entity, by adding additional allocations where needed.
     */
    public static ResourceDimension alignUp(ResourceDimension source, ResourceDimension reference) {
        double cpuRatio = source.getCpu() / reference.getCpu();
        double memoryRatio = source.getMemoryMB() / reference.getMemoryMB();

        if (cpuRatio == memoryRatio) {
            return source;
        }
        if (cpuRatio > memoryRatio) {
            return ResourceDimension.newBuilder()
                    .withCpus(source.getCpu())
                    .withMemoryMB((long) (reference.getMemoryMB() * cpuRatio))
                    .withDiskMB((long) (reference.getDiskMB() * cpuRatio))
                    .withNetworkMbs((long) (reference.getNetworkMbs() * cpuRatio))
                    .withGpu((long) (reference.getGpu() * cpuRatio))
                    .withOpportunisticCpus((long) (reference.getOpportunisticCpu() * cpuRatio))
                    .build();
        }

        // memoryRatio > cpuRatio
        return ResourceDimension.newBuilder()
                .withCpus(reference.getCpu() * memoryRatio)
                .withMemoryMB(source.getMemoryMB())
                .withDiskMB((long) (reference.getDiskMB() * memoryRatio))
                .withNetworkMbs((long) (reference.getNetworkMbs() * memoryRatio))
                .withGpu((long) (reference.getGpu() * memoryRatio))
                .withOpportunisticCpus((long) (reference.getOpportunisticCpu() * memoryRatio))
                .build();
    }

    /**
     * Check if all resources from the second argument are below or equal to resources from the first argument.
     */
    public static boolean isBigger(ResourceDimension dimension, ResourceDimension subDimension) {
        return dimension.getCpu() >= subDimension.getCpu() &&
                dimension.getMemoryMB() >= subDimension.getMemoryMB() &&
                dimension.getDiskMB() >= subDimension.getDiskMB() &&
                dimension.getNetworkMbs() >= subDimension.getNetworkMbs() &&
                dimension.getGpu() >= subDimension.getGpu() &&
                dimension.getOpportunisticCpu() >= subDimension.getOpportunisticCpu();
    }

    public static StringBuilder format(ResourceDimension input, StringBuilder output) {
        output.append("[cpu=").append(input.getCpu());
        output.append(", memoryMB=").append(input.getMemoryMB());
        output.append(", diskMB=").append(input.getDiskMB());
        output.append(", networkMbs=").append(input.getNetworkMbs());
        output.append(", gpu=").append(input.getGpu());
        output.append(", opportunisticCpu=").append(input.getOpportunisticCpu());
        output.append(']');
        return output;
    }

    public static String format(ResourceDimension input) {
        return format(input, new StringBuilder()).toString();
    }

    public static ResourceDimension fromAwsInstanceType(AwsInstanceType instanceType) {
        AwsInstanceDescriptor descriptor = instanceType.getDescriptor();
        return ResourceDimension.newBuilder()
                .withCpus(descriptor.getvCPUs())
                .withGpu(descriptor.getvGPUs())
                .withMemoryMB(descriptor.getMemoryGB() * 1024)
                .withDiskMB(descriptor.getStorageGB() * 1024)
                .withNetworkMbs(descriptor.getNetworkMbs())
                .build();
    }
}
