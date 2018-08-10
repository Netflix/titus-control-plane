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

package com.netflix.titus.ext.zookeeper.mesos;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.ext.zookeeper.connector.ZookeeperClusterResolver;
import com.netflix.titus.ext.zookeeper.ZookeeperPaths;
import com.netflix.titus.master.mesos.MesosMasterResolver;

/**
 * Resolve Mesos address from Zookeeper. As Mesos addresses are not configured directly, both {@link #resolveLeader()},
 * and {@link #resolveMesosAddresses()} return empty result.
 */
@Singleton
public class ZkMesosMasterResolver implements MesosMasterResolver {

    private final ZookeeperClusterResolver zookeeperClusterResolver;
    private final ZookeeperPaths zkPaths;

    @Inject
    public ZkMesosMasterResolver(ZookeeperPaths zkPaths, ZookeeperClusterResolver zookeeperClusterResolver) {
        this.zkPaths = zkPaths;
        this.zookeeperClusterResolver = zookeeperClusterResolver;
    }

    @Override
    public Optional<String> resolveCanonical() {
        Optional<String> zkAddress = zookeeperClusterResolver.resolve();
        return zkAddress.map(s -> "zk://" + s + zkPaths.getMesosPath());
    }

    @Override
    public Optional<InetSocketAddress> resolveLeader() {
        return Optional.empty();
    }

    @Override
    public List<InetSocketAddress> resolveMesosAddresses() {
        return Collections.emptyList();
    }
}
