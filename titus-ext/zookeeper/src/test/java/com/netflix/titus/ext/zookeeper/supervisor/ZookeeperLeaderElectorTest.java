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
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.ext.zookeeper.CuratorServiceResource;
import com.netflix.titus.ext.zookeeper.ZookeeperConfiguration;
import com.netflix.titus.ext.zookeeper.ZookeeperPaths;
import com.netflix.titus.ext.zookeeper.connector.CuratorServiceImpl;
import com.netflix.titus.ext.zookeeper.connector.CuratorUtils;
import com.netflix.titus.ext.zookeeper.connector.DefaultZookeeperClusterResolver;
import com.netflix.titus.ext.zookeeper.connector.ZookeeperClusterResolver;
import com.netflix.titus.master.supervisor.service.LeaderActivator;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import com.netflix.titus.testkit.junit.category.IntegrationNotParallelizableTest;
import com.netflix.titus.testkit.junit.resource.CloseableExternalResource;
import org.apache.curator.CuratorConnectionLossException;
import org.apache.curator.framework.CuratorFramework;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(IntegrationNotParallelizableTest.class)
public class ZookeeperLeaderElectorTest {

    private static final TitusRuntime titusRuntime = TitusRuntimes.internal();

    @ClassRule
    public static final CuratorServiceResource curatorServiceResource = new CuratorServiceResource(titusRuntime);

    @Rule
    public final CloseableExternalResource closeable = new CloseableExternalResource();

    private final LeaderActivator leaderActivator = mock(LeaderActivator.class);

    private final MasterDescription masterDescription = new MasterDescription(
            "myHost", "1.1.1.1", 8080, "/api/status/uri", System.currentTimeMillis()
    );

    private final CuratorFramework curator = curatorServiceResource.getCuratorService().getCurator();
    private final ZookeeperPaths zkPaths = curatorServiceResource.getZkPaths();

    @Test
    public void testConnectionLossWillLeadToStartupFailure() {
        ZookeeperConfiguration config = mock(ZookeeperConfiguration.class);
        when(config.getZkConnectionString()).thenReturn("127.0.0.1:44444");

        ZookeeperClusterResolver clusterResolver = new DefaultZookeeperClusterResolver(config);
        try {
            CuratorServiceImpl cs = closeable.autoCloseable(new CuratorServiceImpl(config, clusterResolver, new DefaultRegistry()), CuratorServiceImpl::shutdown);
            cs.start();

            ZookeeperLeaderElector elector = new ZookeeperLeaderElector(leaderActivator, cs, zkPaths, masterDescription, titusRuntime);
            elector.join();
            fail("The elector should fail fast");
        } catch (IllegalStateException e) {
            assertEquals("The cause should be from ZK connection failure", CuratorConnectionLossException.class, e.getCause().getClass());
            assertTrue("The error message is unexpected: " + e.getMessage(), e.getCause().getMessage().contains("ConnectionLoss"));
        }
    }

    @Test
    public void testLeaderCanHandleExistingPath() throws Exception {
        CuratorUtils.createPathIfNotExist(curator, zkPaths.getLeaderElectionPath(), true);
        CuratorUtils.createPathIfNotExist(curator, zkPaths.getLeaderAnnouncementPath(), true);

        ZookeeperLeaderElector elector = newZookeeperLeaderElector();
        assertThat(elector.join()).isTrue();

        awaitLeaderActivation();
    }

    @Test
    public void testLeaveIfNotLeader() throws Exception {
        ZookeeperLeaderElector firstElector = newZookeeperLeaderElector();
        ZookeeperLeaderElector secondElector = newZookeeperLeaderElector();

        // Make the first elector a leader.
        assertThat(firstElector.join()).isTrue();
        awaitLeaderActivation();
        assertThat(firstElector.leaveIfNotLeader()).isFalse();

        // Check that second elector can join and leave the leader election process.
        assertThat(secondElector.join()).isTrue();
        assertThat(secondElector.leaveIfNotLeader()).isTrue();
    }

    private ZookeeperLeaderElector newZookeeperLeaderElector() {
        return closeable.autoCloseable(
                new ZookeeperLeaderElector(leaderActivator, curatorServiceResource.getCuratorService(), zkPaths, masterDescription, titusRuntime),
                ZookeeperLeaderElector::shutdown
        );
    }

    private void awaitLeaderActivation() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(leaderActivator).becomeLeader();

        latch.await(5, TimeUnit.SECONDS);
        verify(leaderActivator, times(1)).becomeLeader();
    }
}
