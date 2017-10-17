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

package io.netflix.titus.master.master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.master.cluster.ZkExternalResource;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.zookeeper.CuratorServiceImpl;
import io.netflix.titus.master.zookeeper.DefaultZookeeperClusterResolver;
import io.netflix.titus.master.zookeeper.ZookeeperClusterResolver;
import io.netflix.titus.master.zookeeper.ZookeeperPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import rx.functions.Action1;
import rx.functions.Func1;

import static io.netflix.titus.master.ConfigurationMockSamples.withEmbeddedZookeeper;
import static io.netflix.titus.master.ConfigurationMockSamples.withExecutionEnvironment;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ZookeeperMasterMonitorTest {

    private static TemporaryFolder tempFolder = new TemporaryFolder();
    private static ZkExternalResource zkServer = new ZkExternalResource(tempFolder);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(tempFolder)
            .around(zkServer);

    private static MasterConfiguration config;

    private final ZookeeperClusterResolver clusterResolver = new DefaultZookeeperClusterResolver(config);

    private ZookeeperPaths zkPaths = new ZookeeperPaths("/titus/unittest");

    @BeforeClass
    public static void setUp() throws Exception {
        config = withExecutionEnvironment(withEmbeddedZookeeper(mock(MasterConfiguration.class), zkServer.getZkConnStr()));
    }

    public MasterDescription newMasterDescription() {
        String host = getHost();

        return new MasterDescription(
                host,
                getHostIP(),
                config.getApiProxyPort(),
                config.getApiStatusUri(),
                System.currentTimeMillis()
        );
    }

    private String getHost() {
        String host = config.getMasterHost();
        if (host != null) {
            return host;
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }

    private String getHostIP() {
        String ip = config.getMasterIP();
        if (ip != null) {
            return ip;
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }

    @Test
    public void testMonitorWorksForMultipleUpdates() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        int total = 5;
        final CountDownLatch latch = new CountDownLatch(total);
        final CuratorServiceImpl curatorService = new CuratorServiceImpl(config, clusterResolver, new DefaultRegistry());
        final ZookeeperMasterMonitor masterMonitor = new ZookeeperMasterMonitor(curatorService, zkPaths);

        // Note we intentionally didn't set the initial value of master description because we'd like to make sure
        // that the monitor will work property even if it fails occasionally (in this case, it will fail to deserialize
        // the master description in the very beginning
        masterMonitor.getMasterObservable()
                .filter(new Func1<MasterDescription, Boolean>() {
                    @Override
                    public Boolean call(MasterDescription masterDescription) {
                        return masterDescription != null;
                    }
                })
                .doOnNext(new Action1<MasterDescription>() {
                    @Override
                    public void call(MasterDescription masterDescription) {
                        System.out.println(counter.incrementAndGet() + ": Got new master: " + masterDescription.toString());
                        latch.countDown();
                    }
                })
                .subscribe();
        curatorService.start();
        masterMonitor.start();

        try {
            CuratorFramework curator = curatorService.getCurator();
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

            // Make enough updates to make sure that the monitor can pick up sufficient number of changes
            // We use twice updates as needed in case some of the updates time out.
            for (int i = 0; i < 2 * total; ++i) {
                curatorService.getCurator().setData()
                        .forPath(zkPaths.getLeaderAnnouncementPath(), ObjectMappers.defaultMapper().writeValueAsBytes(newMasterDescription()));
                Thread.sleep(1000);
            }

            try {
                boolean success = latch.await(60, TimeUnit.SECONDS);
                assertTrue(String.format("The monitor should have picked up %d updates, but only %d is reported ", total, total - latch.getCount()), success);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } finally {
            curatorService.shutdown();
        }
    }
}