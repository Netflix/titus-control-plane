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

package com.netflix.titus.ext.zookeeper;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.ext.zookeeper.connector.CuratorService;
import com.netflix.titus.ext.zookeeper.connector.CuratorServiceImpl;
import com.netflix.titus.ext.zookeeper.connector.CuratorUtils;
import com.netflix.titus.ext.zookeeper.connector.DefaultZookeeperClusterResolver;
import com.netflix.titus.ext.zookeeper.connector.ZookeeperClusterResolver;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.mockito.Mockito;

public class CuratorServiceResource extends ExternalResource {

    private final TitusRuntime titusRuntime;

    private ZookeeperPaths zkPaths;

    private CuratorServiceImpl curatorService;

    private TestingServer zkServer;

    public CuratorServiceResource(TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
    }

    @Override
    protected void before() throws Throwable {
        zkServer = new TestingServer(true);

        ZookeeperConfiguration zookeeperConfiguration = ZookeeperTestUtils.withEmbeddedZookeeper(Mockito.mock(ZookeeperConfiguration.class), zkServer.getConnectString());
        zkPaths = new ZookeeperPaths(zookeeperConfiguration);

        ZookeeperClusterResolver clusterResolver = new DefaultZookeeperClusterResolver(zookeeperConfiguration);
        curatorService = new CuratorServiceImpl(zookeeperConfiguration, clusterResolver, titusRuntime.getRegistry());

        curatorService.start();
    }

    @Override
    protected void after() {
        curatorService.shutdown();
        ExceptionExt.silent(() -> zkServer.close());
    }

    public void createAllPaths() {
        CuratorFramework curator = curatorService.getCurator();
        CuratorUtils.createPathIfNotExist(curator, zkPaths.getLeaderElectionPath(), true);
        CuratorUtils.createPathIfNotExist(curator, zkPaths.getLeaderAnnouncementPath(), true);
    }

    public CuratorService getCuratorService() {
        return curatorService;
    }

    public ZookeeperPaths getZkPaths() {
        return zkPaths;
    }
}
