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

package io.netflix.titus.master.model;

import java.util.Collection;

import com.google.common.base.Preconditions;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.agent.ServerInfo;

import static io.netflix.titus.common.util.CollectionsExt.isNullOrEmpty;

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
        int memorySum = 0;
        int diskSum = 0;
        int networkSum = 0;
        for (ResourceDimension part : parts) {
            cpuSum += part.getCpu();
            memorySum += part.getMemoryMB();
            diskSum += part.getDiskMB();
            networkSum += part.getNetworkMbs();
        }
        return ResourceDimension.newBuilder()
                .withCpus(cpuSum)
                .withMemoryMB(memorySum)
                .withDiskMB(diskSum)
                .withNetworkMbs(networkSum)
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
                .build();
    }

    /**
     * Multiple all resources of the {@link ResourceDimension} instance by the given number.
     */
    public static ResourceDimension multiply(ResourceDimension base, double multiplier) {
        return ResourceDimension.newBuilder()
                .withCpus(base.getCpu() * multiplier)
                .withMemoryMB((int) Math.ceil(base.getMemoryMB() * multiplier))
                .withDiskMB((int) Math.ceil(base.getDiskMB() * multiplier))
                .withNetworkMbs((int) Math.ceil(base.getNetworkMbs() * multiplier))
                .build();
    }

    public static Pair<Integer, ResourceDimension> divide(ResourceDimension left, ResourceDimension right) {
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

        if (multiplier == 0) { // left is empty
            return Pair.of(0, ResourceDimension.empty());
        }
        if (multiplier < 1) { // left < right
            return Pair.of(0, left);
        }
        if (multiplier == 1) {
            return Pair.of(1, ResourceDimension.empty());
        }

        int full = (int) multiplier;

        return Pair.of(
                full,
                ResourceDimension.newBuilder()
                        .withCpus(Math.max(0, left.getCpu() - right.getCpu() * full))
                        .withMemoryMB(Math.max(0, left.getMemoryMB() - right.getMemoryMB() * full))
                        .withDiskMB(Math.max(0, left.getDiskMB() - right.getDiskMB() * full))
                        .withNetworkMbs(Math.max(0, left.getNetworkMbs() - right.getNetworkMbs() * full))
                        .build()
        );
    }

    public static int divideAndRoundUp(ResourceDimension left, ResourceDimension right) {
        Pair<Integer, ResourceDimension> result = divide(left, right);
        return result.getRight().equals(ResourceDimension.empty()) ? result.getLeft() : result.getLeft() + 1;
    }

    /**
     * Align source {@link ResourceDimension} resources, to match resource ratios from the reference entity, by
     * adding additional allocations where needed.
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
                    .withMemoryMB((int) (reference.getMemoryMB() * cpuRatio))
                    .withDiskMB((int) (reference.getDiskMB() * cpuRatio))
                    .withNetworkMbs((int) (reference.getNetworkMbs() * cpuRatio))
                    .build();
        }

        // memoryRatio > cpuRatio
        return ResourceDimension.newBuilder()
                .withCpus(reference.getCpu() * memoryRatio)
                .withMemoryMB(source.getMemoryMB())
                .withDiskMB((int) (reference.getDiskMB() * memoryRatio))
                .withNetworkMbs((int) (reference.getNetworkMbs() * memoryRatio))
                .build();
    }

    /**
     * Check if all resources from the second argument are below or equal to resources from the first argument.
     */
    public static boolean isBigger(ResourceDimension dimension, ResourceDimension subDimension) {
        if (dimension.getCpu() < subDimension.getCpu()) {
            return false;
        }
        if (dimension.getMemoryMB() < subDimension.getMemoryMB()) {
            return false;
        }
        if (dimension.getDiskMB() < subDimension.getDiskMB()) {
            return false;
        }
        if (dimension.getNetworkMbs() < subDimension.getNetworkMbs()) {
            return false;
        }
        return true;
    }

    public static StringBuilder format(ResourceDimension input, StringBuilder output) {
        output.append("[cpu=").append(input.getCpu());
        output.append(", memoryMB=").append(input.getMemoryMB());
        output.append(", diskMB=").append(input.getDiskMB());
        output.append(", networkMbs=").append(input.getNetworkMbs());
        output.append(']');
        return output;
    }

    public static String format(ResourceDimension input) {
        return format(input, new StringBuilder()).toString();
    }

    public static ResourceDimension fromServerInfo(ServerInfo serverInfo) {
        return ResourceDimension.newBuilder()
                .withCpus(serverInfo.getCpus())
                .withMemoryMB(serverInfo.getMemoryGB() * 1024)
                .withDiskMB(serverInfo.getStorageGB() * 1024)
                .withNetworkMbs(serverInfo.getNetworkMbs())
                .build();
    }

    public static ResourceDimension fromAwsInstanceType(AwsInstanceType instanceType) {
        AwsInstanceDescriptor descriptor = instanceType.getDescriptor();
        return ResourceDimension.newBuilder()
                .withCpus(descriptor.getvCPUs())
                .withMemoryMB(descriptor.getMemoryGB() * 1024)
                .withDiskMB(descriptor.getStorageGB() * 1024)
                .withNetworkMbs(descriptor.getNetworkMbs())
                .build();
    }

    public static ResAllocs toResAllocs(String name, ResourceDimension resourceDimension) {
        return new ResAllocsBuilder(name)
                .withCores(resourceDimension.getCpu())
                .withMemory(resourceDimension.getMemoryMB())
                .withDisk(resourceDimension.getDiskMB())
                .withNetworkMbps(resourceDimension.getNetworkMbs())
                .build();
    }

    public static ResourceDimension fromResAllocs(ResAllocs resAllocs) {
        return ResourceDimension.newBuilder()
                .withCpus(resAllocs.getCores())
                .withMemoryMB((int) resAllocs.getMemory())
                .withDiskMB((int) resAllocs.getDisk())
                .withNetworkMbs((int) resAllocs.getNetworkMbps())
                .build();
    }
}
