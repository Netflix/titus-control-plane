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

package com.netflix.titus.master.cluster;

import java.io.IOException;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.master.master.MasterDescription;
import com.netflix.titus.master.zookeeper.CuratorService;
import com.netflix.titus.master.zookeeper.ZookeeperPaths;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.KeeperException.Code.OK;

@Singleton
public class DefaultLeaderElector implements LeaderElector {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLeaderElector.class);

    private volatile boolean started = false;

    private final LeaderActivator leaderActivator;
    private final ObjectMapper jsonMapper;
    private final MasterDescription masterDescription;
    private final LeaderLatch leaderLatch;
    private final CuratorFramework curator;
    // The path where a selected leader announces itself.
    private final String leaderPath;

    @Inject
    public DefaultLeaderElector(LeaderActivator leaderActivator,
                                CuratorService curatorService,
                                ZookeeperPaths zookeeperPaths,
                                MasterDescription masterDescription) {
        this.leaderActivator = leaderActivator;
        this.jsonMapper = ObjectMappers.defaultMapper();
        this.masterDescription = masterDescription;
        this.curator = curatorService.getCurator();
        this.leaderLatch = createNewLeaderLatch(zookeeperPaths.getLeaderElectionPath());
        this.leaderPath = zookeeperPaths.getLeaderAnnouncementPath();
    }

    @PostConstruct
    public void start() {
        if (started) {
            return;
        }
        started = true;

        try {
            Stat pathStat = curator.checkExists().forPath(leaderPath);
            // Create the path only if the path does not exist
            if (pathStat == null) {
                curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(leaderPath);
            }

            leaderLatch.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create a leader elector for master: " + e.getMessage(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            leaderLatch.close();
        } catch (IOException e) {
            logger.warn("Failed to close the leader latch: {}", e.getMessage(), e);
        } finally {
            started = false;
        }
    }

    private LeaderLatch createNewLeaderLatch(String leaderPath) {
        final LeaderLatch newLeaderLatch = new LeaderLatch(curator, leaderPath, "127.0.0.1");

        newLeaderLatch.addListener(
                new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        announceLeader();
                    }

                    @Override
                    public void notLeader() {
                        leaderActivator.stopBeingLeader();
                    }
                }, Executors.newSingleThreadExecutor(new DefaultThreadFactory("MasterLeader-%s")));

        return newLeaderLatch;
    }

    private void announceLeader() {
        try {
            logger.info("Announcing leader");
            byte[] masterDescription = jsonMapper.writeValueAsBytes(this.masterDescription);

            // There is no need to lock anything because we ensure only leader will write to the leader path
            curator
                    .setData()
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                            if (event.getResultCode() == OK.intValue()) {
                                leaderActivator.becomeLeader();
                            } else {
                                logger.warn("Failed to elect leader from path {} with event {}", leaderPath, event);
                            }
                        }
                    }).forPath(leaderPath, masterDescription);
        } catch (Exception e) {
            throw new RuntimeException("Failed to announce leader: " + e.getMessage(), e);
        }
    }
}
