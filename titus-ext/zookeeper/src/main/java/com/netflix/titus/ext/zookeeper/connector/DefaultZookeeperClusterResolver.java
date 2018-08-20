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

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.ext.zookeeper.ZookeeperConfiguration;

/**
 * Default implementation reads Zookeeper connection string from configuration.
 * More sophisticated implementations might resolve this information from DNS or other name service (Eureka).
 */
@Singleton
public class DefaultZookeeperClusterResolver implements ZookeeperClusterResolver {

    private final String zkConnectionString;

    @Inject
    public DefaultZookeeperClusterResolver(ZookeeperConfiguration config) {
        zkConnectionString = config.getZkConnectionString();
    }

    @Override
    public Optional<String> resolve() {
        return Optional.of(zkConnectionString);
    }
}
