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

package io.netflix.titus.master.appscale.endpoint.v3;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import io.netflix.titus.api.appscale.service.AppScaleManager;
import io.netflix.titus.api.connector.cloud.AppAutoScalingClient;
import io.netflix.titus.api.connector.cloud.CloudAlarmClient;
import io.netflix.titus.api.connector.cloud.noop.NoOpAppAutoScalingClient;
import io.netflix.titus.api.connector.cloud.noop.NoOpCloudAlarmClient;
import io.netflix.titus.master.appscale.endpoint.v3.grpc.DefaultAutoScalingServiceGrpc;
import io.netflix.titus.master.appscale.service.AppScaleManagerConfiguration;
import io.netflix.titus.master.appscale.service.DefaultAppScaleManager;

public class AutoScalingModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AutoScalingServiceGrpc.AutoScalingServiceImplBase.class).to(DefaultAutoScalingServiceGrpc.class);
        bind(AppScaleManager.class).to(DefaultAppScaleManager.class);
        bind(CloudAlarmClient.class).to(NoOpCloudAlarmClient.class);
        bind(AppAutoScalingClient.class).to(NoOpAppAutoScalingClient.class);
    }

    @Provides
    @Singleton
    public AppScaleManagerConfiguration getAppScaleManagerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(AppScaleManagerConfiguration.class);
    }
}
