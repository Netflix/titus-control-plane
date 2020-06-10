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

package com.netflix.titus.ext.aws;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;

@Singleton
public class DataPlaneAmazonAutoScalingAsyncProvider extends AmazonAutoScalingAsyncProvider {

    public static final String NAME = "dataPlaneAmazonAutoScalingClient";

    private final AmazonAutoScalingAsync amazonAutoScaling;

    @Inject
    public DataPlaneAmazonAutoScalingAsyncProvider(AwsConfiguration configuration, AWSCredentialsProvider credentialProvider) {
        String region = AwsRegionConfigurationUtil.resolveDataPlaneRegion(configuration);
        this.amazonAutoScaling = buildAmazonAutoScalingAsyncClient(region, credentialProvider);
    }

    @Override
    public AmazonAutoScalingAsync get() {
        return amazonAutoScaling;
    }

    @PreDestroy
    public void shutdown() {
        amazonAutoScaling.shutdown();
    }
}
