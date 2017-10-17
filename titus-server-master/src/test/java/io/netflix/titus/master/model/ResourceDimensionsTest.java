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

import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.common.aws.AwsInstanceDescriptor;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import io.netflix.titus.testkit.data.core.ResourceDimensionSample;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ResourceDimensionsTest {

    @Test
    public void testResourceDimensionAddition() throws Exception {
        ResourceDimension small = ResourceDimensionSample.Small.build();
        ResourceDimension expected = ResourceDimensionSample.SmallX2.build();

        assertThat(ResourceDimensions.add(small, small)).isEqualTo(expected);
    }

    @Test
    public void testResourceDimensionSubtraction() throws Exception {
        ResourceDimension large = ResourceDimensionSample.SmallX2.build();
        ResourceDimension small = ResourceDimensionSample.Small.build();

        assertThat(ResourceDimensions.subtractPositive(large, small)).isEqualTo(small);
    }

    @Test
    public void testResourceDimensionMultiplication() throws Exception {
        ResourceDimension small = ResourceDimensionSample.Small.build();
        ResourceDimension expected = ResourceDimensionSample.SmallX2.build();

        assertThat(ResourceDimensions.multiply(small, 2)).isEqualTo(expected);
    }

    @Test
    public void testResourceDimensionDivide() throws Exception {
        ResourceDimension large = ResourceDimensionSample.SmallX2.build();
        ResourceDimension small = ResourceDimensionSample.Small.build();

        Pair<Integer, ResourceDimension> result = ResourceDimensions.divide(large, small);
        assertThat(result.getLeft()).isEqualTo(2);
        assertThat(result.getRight()).isEqualTo(ResourceDimension.empty());
    }

    @Test
    public void testResourceDimensionDivideAndRoundUp() throws Exception {
        ResourceDimension large = ResourceDimensionSample.SmallX2.build();
        ResourceDimension largePlus = ResourceDimensionSample.SmallX2.builder().withCpus(large.getCpu() + 1).build();
        ResourceDimension small = ResourceDimensionSample.Small.build();

        assertThat(ResourceDimensions.divideAndRoundUp(large, small)).isEqualTo(2);
        assertThat(ResourceDimensions.divideAndRoundUp(largePlus, small)).isEqualTo(3);
    }

    @Test
    public void testAligningUpToHigherCPU() throws Exception {
        ResourceDimension small2X = ResourceDimensionSample.SmallX2.build();
        ResourceDimension original = ResourceDimensionSample.Small.builder().withCpus(small2X.getCpu()).build();

        assertThat(ResourceDimensions.alignUp(original, small2X)).isEqualTo(small2X);
    }

    @Test
    public void testAligningUpToHigherMemory() throws Exception {
        ResourceDimension small2X = ResourceDimensionSample.SmallX2.build();
        ResourceDimension original = ResourceDimensionSample.Small.builder().withMemoryMB(small2X.getMemoryMB()).build();

        assertThat(ResourceDimensions.alignUp(original, small2X)).isEqualTo(small2X);
    }

    @Test
    public void testFromServerInfoMapping() throws Exception {
        ServerInfo m42xl = ServerInfoResolvers.fromAwsInstanceTypes()
                .resolve(AwsInstanceType.M4_2XLarge.getDescriptor().getId())
                .get();
        ResourceDimension actual = ResourceDimensions.fromServerInfo(m42xl);
        AwsInstanceDescriptor descriptor = AwsInstanceType.M4_2XLarge.getDescriptor();

        assertThat(actual.getCpu()).isEqualTo(descriptor.getvCPUs());
        assertThat(actual.getMemoryMB()).isEqualTo(descriptor.getMemoryGB() * 1024);
        assertThat(actual.getDiskMB()).isEqualTo(descriptor.getStorageGB() * 1024);
        assertThat(actual.getNetworkMbs()).isEqualTo(descriptor.getNetworkMbs());
    }
}