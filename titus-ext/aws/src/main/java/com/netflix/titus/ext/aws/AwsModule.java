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

import javax.inject.Singleton;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsync;
import com.amazonaws.services.ec2.AmazonEC2Async;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsync;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.ext.aws.appscale.AWSAppScalingConfig;

public class AwsModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AmazonEC2Async.class).toProvider(AmazonEC2AsyncProvider.class);
        bind(AmazonAutoScalingAsync.class).toProvider(AmazonAutoScalingAsyncProvider.class);
        bind(AmazonElasticLoadBalancingAsync.class).toProvider(AmazonElasticLoadBalancingAsyncProvider.class);
        bind(AmazonAutoScaling.class).toProvider(AmazonAutoScalingProvider.class);
        bind(InstanceCloudConnector.class).to(AwsInstanceCloudConnector.class);
        bind(InstanceReaper.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public AwsConfiguration getAwsConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(AwsConfiguration.class);
    }

    @Provides
    @Singleton
    public AWSAppScalingConfig getAWSAppScalingConfig(ConfigProxyFactory factory) {
        return factory.newProxy(AWSAppScalingConfig.class);
    }
}
