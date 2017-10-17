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

package io.netflix.titus.ext.aws;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClientBuilder;

@Singleton
public class AmazonAutoScalingAsyncProvider implements Provider<AmazonAutoScalingAsync> {

    private final AmazonAutoScalingAsync amazonAutoScaling;

    @Inject
    public AmazonAutoScalingAsyncProvider(AwsConfiguration configuration, AWSCredentialsProvider credentialProvider) {
        String region = configuration.getRegion().trim().toLowerCase();
        this.amazonAutoScaling = AmazonAutoScalingAsyncClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("autoscaling." + region + ".amazonaws.com", region))
                .withCredentials(credentialProvider)
                .build();
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
