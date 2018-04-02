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

package com.netflix.titus.master.master;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.master.zookeeper.CuratorService;
import com.netflix.titus.master.zookeeper.ZookeeperPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;

/**
 * A monitor that monitors the status of Titus masters.
 */
@Singleton
public class ZookeeperMasterMonitor implements MasterMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMasterMonitor.class);

    private final CuratorFramework curator;
    private final String masterPath;
    private final BehaviorSubject<MasterDescription> masterSubject;
    private final AtomicReference<MasterDescription> latestMaster = new AtomicReference<>();
    private final NodeCache nodeMonitor;

    @Inject
    public ZookeeperMasterMonitor(CuratorService curatorService, ZookeeperPaths zkPaths) {
        this(curatorService.getCurator(), zkPaths, null);
    }

    public ZookeeperMasterMonitor(CuratorFramework curator,
                                  ZookeeperPaths zkPaths,
                                  MasterDescription initValue) {
        this.curator = curator;
        this.masterPath = zkPaths.getLeaderAnnouncementPath();
        this.masterSubject = BehaviorSubject.create(initValue);
        this.nodeMonitor = new NodeCache(curator, masterPath);
        this.latestMaster.set(initValue);
    }

    @PostConstruct
    public void start() {
        nodeMonitor.getListenable().addListener(this::retrieveMaster);
        try {
            nodeMonitor.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start master node monitor: " + e.getMessage(), e);
        }

        logger.info("The ZK master monitor is started");
    }

    private void retrieveMaster() {
        try {
            curator
                    .sync()  // sync with ZK before reading
                    .inBackground(
                            curator
                                    .getData()
                                    .inBackground((client, event) -> {
                                        MasterDescription description = ObjectMappers.defaultMapper().readValue(event.getData(), MasterDescription.class);
                                        logger.info("New master retrieved: " + description);
                                        latestMaster.set(description);
                                        masterSubject.onNext(description);

                                    })
                                    .forPath(masterPath)
                    )
                    .forPath(masterPath);

        } catch (Exception e) {
            logger.error("Failed to retrieve updated master information: " + e.getMessage(), e);
        }
    }

    @Override
    public Observable<MasterDescription> getMasterObservable() {
        return masterSubject;
    }

    @Override
    public MasterDescription getLatestMaster() {
        return latestMaster.get();
    }

    public void shutdown() {
        try {
            nodeMonitor.close();
            logger.info("ZK master monitor is shut down");
        } catch (IOException e) {
            throw new RuntimeException("Failed to close the ZK node monitor: " + e.getMessage(), e);
        }
    }
}