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
import javax.inject.Provider;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClientBuilder;
import com.netflix.spectator.aws.SpectatorRequestMetricCollector;
import com.netflix.titus.common.runtime.TitusRuntime;

@Singleton
class AmazonAutoScalingProvider implements Provider<AmazonAutoScaling> {

    private final AmazonAutoScaling amazonAutoScaling;

    @Inject
    public AmazonAutoScalingProvider(AwsConfiguration coreConfiguration,
                                     AWSCredentialsProvider credentialProvider,
                                     TitusRuntime runtime) {
        String region = coreConfiguration.getRegion().trim().toLowerCase();

        // TODO We need both sync and async versions. Remove casting once sync is no longer needed.
        AmazonAutoScalingAsyncClient amazonAutoScalingClient = (AmazonAutoScalingAsyncClient) AmazonAutoScalingAsyncClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("autoscaling." + region + ".amazonaws.com", region))
                .withCredentials(credentialProvider)
                .withMetricsCollector(new SpectatorRequestMetricCollector(runtime.getRegistry()))
                .build();
        this.amazonAutoScaling = amazonAutoScalingClient;
    }

    @Override
    public AmazonAutoScaling get() {
        return amazonAutoScaling;
    }

    @PreDestroy
    public void shutdown() {
        amazonAutoScaling.shutdown();
    }
}
