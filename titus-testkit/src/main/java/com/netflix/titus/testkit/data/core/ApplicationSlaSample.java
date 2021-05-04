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

package com.netflix.titus.testkit.data.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.math.DoubleMath;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceDescriptor;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.model.ResourceDimensions;

import static com.netflix.titus.master.endpoint.v2.rest.ApplicationSlaManagementEndpoint.DEFAULT_APPLICATION;

/**
 * TODO For test readability it would be better to express this with number of CPUs in names
 */
public enum ApplicationSlaSample {

    DefaultFlex() {
        @Override
        public ApplicationSLA.Builder builder() {
            return ApplicationSLA.newBuilder()
                    .withAppName(DEFAULT_APPLICATION)
                    .withTier(Tier.Flex)
                    .withResourceDimension(ResourceDimensionSample.SmallX2.build())
                    .withInstanceCount(100);
        }
    },
    CriticalSmall() {
        @Override
        public ApplicationSLA.Builder builder() {
            return ApplicationSLA.newBuilder()
                    .withAppName("criticalSmallApp")
                    .withTier(Tier.Critical)
                    .withResourceDimension(ResourceDimensionSample.Small.build())
                    .withInstanceCount(2);
        }
    },
    CriticalLarge() {
        @Override
        public ApplicationSLA.Builder builder() {
            return ApplicationSLA.newBuilder()
                    .withAppName("criticalLargeApp")
                    .withTier(Tier.Critical)
                    .withResourceDimension(ResourceDimensionSample.SmallX2.build())
                    .withInstanceCount(10);
        }
    },
    FlexSmall() {
        @Override
        public ApplicationSLA.Builder builder() {
            return ApplicationSLA.newBuilder(CriticalSmall.build())
                    .withAppName("flexSmallApp")
                    .withTier(Tier.Flex)
                    .withInstanceCount(2);
        }
    },
    FlexLarge() {
        @Override
        public ApplicationSLA.Builder builder() {
            return ApplicationSLA.newBuilder(CriticalLarge.build())
                    .withAppName("flexLargeApp")
                    .withTier(Tier.Flex)
                    .withInstanceCount(2);
        }
    },
    FlexSmallKubeScheduler() {
        @Override
        public ApplicationSLA.Builder builder() {
            return ApplicationSLA.newBuilder(CriticalSmall.build())
                    .withAppName("flexSmallKubeSchedulerApp")
                    .withTier(Tier.Flex)
                    .withSchedulerName("kubeScheduler")
                    .withInstanceCount(2);
        }
    },
    CriticalSmallKubeScheduler() {
        @Override
        public ApplicationSLA.Builder builder() {
            return CriticalSmall.builder()
                    .withAppName("criticalSmallKubeSchedulerApp")
                    .withSchedulerName("kubeScheduler")
                    .withResourcePool("reserved");
        }
    };

    public abstract ApplicationSLA.Builder builder();

    public ApplicationSLA build() {
        return builder().build();
    }

    public static List<ApplicationSLA> asList(ApplicationSlaSample... samples) {
        List<ApplicationSLA> sampleList = new ArrayList<>(samples.length);
        for (ApplicationSlaSample s : samples) {
            sampleList.add(s.build());
        }
        return sampleList;
    }

    public static ApplicationSLA fromAwsInstanceType(Tier tier, String appName, AwsInstanceType instanceType, double instanceCount) {
        if (DoubleMath.isMathematicalInteger(instanceCount)) {
            return ApplicationSLA.newBuilder()
                    .withTier(tier)
                    .withAppName(appName)
                    .withInstanceCount((int) instanceCount)
                    .withResourceDimension(ResourceDimensions.fromAwsInstanceType(instanceType))
                    .build();
        }

        // We have a fraction
        AwsInstanceDescriptor descriptor = instanceType.getDescriptor();
        return ApplicationSLA.newBuilder()
                .withAppName(appName)
                .withInstanceCount(1)
                .withResourceDimension(ResourceDimension.newBuilder()
                        .withCpus(descriptor.getvCPUs() * instanceCount)
                        .withMemoryMB((int) (descriptor.getMemoryGB() * 1024 * instanceCount))
                        .withDiskMB((int) (descriptor.getStorageGB() * 1024 * instanceCount))
                        .withNetworkMbs((int) (descriptor.getNetworkMbs() * instanceCount))
                        .build()
                )
                .build();
    }
}
