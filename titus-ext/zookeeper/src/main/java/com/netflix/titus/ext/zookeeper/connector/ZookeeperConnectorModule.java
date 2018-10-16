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

package com.netflix.titus.ext.zookeeper.connector;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.runtime.health.guice.HealthModule;
import com.netflix.titus.ext.zookeeper.ZookeeperConfiguration;

public class ZookeeperConnectorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CuratorService.class).to(CuratorServiceImpl.class);
        bind(ZookeeperClusterResolver.class).to(DefaultZookeeperClusterResolver.class);

        install(new HealthModule() {
            @Override
            protected void configureHealth() {
                bindAdditionalHealthIndicator().to(ZookeeperHealthIndicator.class);
            }
        });
    }

    @Provides
    @Singleton
    public ZookeeperConfiguration getZookeeperConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ZookeeperConfiguration.class);
    }
}
