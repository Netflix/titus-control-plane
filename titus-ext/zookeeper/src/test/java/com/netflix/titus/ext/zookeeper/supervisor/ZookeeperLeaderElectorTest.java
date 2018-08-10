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

package com.netflix.titus.ext.zookeeper.supervisor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.ext.zookeeper.ConfigurationMockSamples;
import com.netflix.titus.ext.zookeeper.ZkExternalResource;
import com.netflix.titus.ext.zookeeper.ZookeeperConfiguration;
import com.netflix.titus.ext.zookeeper.ZookeeperPaths;
import com.netflix.titus.ext.zookeeper.connector.CuratorServiceImpl;
import com.netflix.titus.ext.zookeeper.connector.DefaultZookeeperClusterResolver;
import com.netflix.titus.ext.zookeeper.connector.ZookeeperClusterResolver;
import com.netflix.titus.master.supervisor.service.LeaderActivator;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import com.netflix.titus.testkit.junit.category.IntegrationNotParallelizableTest;
import org.apache.curator.CuratorConnectionLossException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(IntegrationNotParallelizableTest.class)
public class ZookeeperLeaderElectorTest {

    private static TemporaryFolder tempFolder = new TemporaryFolder();
    private static ZkExternalResource zkServer = new ZkExternalResource(tempFolder);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(tempFolder)
            .around(zkServer);

    private ZookeeperConfiguration config;

    private final LeaderActivator leaderActivator = mock(LeaderActivator.class);
    private final MasterDescription masterDescription = new MasterDescription(
            "myHost", "1.1.1.1", 8080, "/api/status/uri", System.currentTimeMillis()
    );

    private ZookeeperPaths zkPaths;

    @Before
    public void setUp() {
        config = ConfigurationMockSamples.withEmbeddedZookeeper(mock(ZookeeperConfiguration.class), zkServer.getZkConnStr());
        zkPaths = new ZookeeperPaths(config);
    }

    @Test
    public void testConnectionLossWillLeadToStartupFailure() throws Exception {
        when(config.getZkConnectionString()).thenReturn("non-existent:2181");
        ZookeeperClusterResolver clusterResolver = new DefaultZookeeperClusterResolver(config);
        CuratorServiceImpl cs = null;
        try {
            cs = new CuratorServiceImpl(config, clusterResolver, new DefaultRegistry());
            cs.start();

            ZookeeperLeaderElector elector = new ZookeeperLeaderElector(leaderActivator, cs, zkPaths, masterDescription);
            elector.join();
            fail("The elector should fail fast");
        } catch (IllegalStateException e) {
            assertEquals("The cause should be from ZK connection failure", CuratorConnectionLossException.class, e.getCause().getClass());
            assertTrue("The error message is unexpected: " + e.getMessage(), e.getMessage().contains("ConnectionLoss"));
        } finally {
            if (cs != null) {
                cs.shutdown();
            }
        }
    }

    @Test
    public void testLeaderCanHandleExistingPath() throws Exception {
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .compressionProvider(new GzipCompressionProvider())
                .connectionTimeoutMs(config.getZkConnectionTimeoutMs())
                .retryPolicy(new ExponentialBackoffRetry(config.getZkConnectionRetrySleepMs(), config.getZkConnectionMaxRetries()))
                .connectString(config.getZkConnectionString())
                .build();
        ZookeeperLeaderElector elector = null;

        try {
            curator.start();

            for (String fullPath : new String[]{zkPaths.getLeaderElectionPath(), zkPaths.getLeaderAnnouncementPath()}) {
                Stat pathStat = curator.checkExists().forPath(fullPath);
                // Create the path only if the path does not exist
                if (pathStat == null) {
                    curator.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(fullPath);
                }
            }

            final CountDownLatch latch = new CountDownLatch(1);
            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(leaderActivator).becomeLeader();

            elector = new ZookeeperLeaderElector(leaderActivator, () -> curator, zkPaths, masterDescription);
            elector.join();

            latch.await(5, TimeUnit.SECONDS);
            verify(leaderActivator).becomeLeader();
        } finally {
            if (elector != null) {
                elector.shutdown();
            }
            if (curator != null) {
                curator.close();
            }
        }
    }
}
