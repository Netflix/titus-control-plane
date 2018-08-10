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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.ext.zookeeper.ZookeeperPaths;
import com.netflix.titus.ext.zookeeper.connector.CuratorService;
import com.netflix.titus.ext.zookeeper.connector.CuratorUtils;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import com.netflix.titus.master.supervisor.service.MasterMonitor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.subjects.BehaviorSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import static com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters.toCoreMasterInstance;
import static com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters.toGrpcMasterInstance;

/**
 * A monitor that monitors the status of Titus masters.
 */
@Singleton
public class ZookeeperMasterMonitor implements MasterMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMasterMonitor.class);

    /**
     * During the system start time the full information about the local instance state is not known, so
     * instead we assign a default value which indicates this fact. Before the bootstrap completes this default
     * should be replaced with a correct value, or the JVM process should terminate.
     */
    private static final MasterInstance UNKNOWN_MASTER_INSTANCE = MasterInstance.newBuilder()
            .withInstanceId("unknownId")
            .withIpAddress("0.0.0.0")
            .withStatus(MasterStatus.newBuilder()
                    .withState(MasterState.Starting)
                    .withReasonCode("unresolvedMasterState")
                    .withReasonMessage("TitusMaster instance state not known yet")
                    .build()
            )
            .build();

    private final CuratorFramework curator;
    private final TitusRuntime titusRuntime;

    private final String leaderPath;
    private final BehaviorSubject<MasterDescription> leaderSubject;
    private final AtomicReference<MasterDescription> latestLeader = new AtomicReference<>();
    private final NodeCache leaderMonitor;

    private final String masterPath;
    private final TreeCache masterMonitor;
    private final Subject<List<MasterInstance>, List<MasterInstance>> masterUpdates = new SerializedSubject<>(
            BehaviorSubject.create(Collections.emptyList())
    );
    private final Observable<List<MasterInstance>> masterUpdatesObserver = ObservableExt.protectFromMissingExceptionHandlers(
            masterUpdates.asObservable(),
            logger
    );
    private volatile MasterInstance ownMasterInstance = UNKNOWN_MASTER_INSTANCE;

    @Inject
    public ZookeeperMasterMonitor(ZookeeperPaths zkPaths, CuratorService curatorService, TitusRuntime titusRuntime) {
        this(zkPaths, curatorService.getCurator(), null, titusRuntime);
    }

    public ZookeeperMasterMonitor(ZookeeperPaths zkPaths,
                                  CuratorFramework curator,
                                  MasterDescription initValue,
                                  TitusRuntime titusRuntime) {
        this.curator = curator;
        this.titusRuntime = titusRuntime;

        this.leaderPath = zkPaths.getLeaderAnnouncementPath();
        this.leaderSubject = BehaviorSubject.create(initValue);
        this.leaderMonitor = new NodeCache(curator, leaderPath);
        this.latestLeader.set(initValue);

        this.masterPath = zkPaths.getTitusMasterPath();
        this.masterMonitor = new TreeCache(curator, masterPath);
    }

    @PostConstruct
    public void start() {
        leaderMonitor.getListenable().addListener(this::retrieveLeader);
        masterMonitor.getListenable().addListener(this::retrieveAllMasters);
        try {
            leaderMonitor.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start master node monitor: " + e.getMessage(), e);
        }

        logger.info("The ZK master monitor is started");
    }

    @PreDestroy
    public void shutdown() {
        try {
            leaderMonitor.close();
            logger.info("ZK master monitor is shut down");
        } catch (IOException e) {
            throw new RuntimeException("Failed to close the ZK node monitor: " + e.getMessage(), e);
        }
    }


    @Override
    public Observable<MasterDescription> getLeaderObservable() {
        return leaderSubject;
    }

    @Override
    public MasterDescription getLatestLeader() {
        return latestLeader.get();
    }

    @Override
    public MasterInstance getCurrentMasterInstance() {
        return ownMasterInstance;
    }

    @Override
    public Completable updateOwnMasterInstance(MasterInstance self) {
        return CuratorUtils.setData(curator, masterPath + '/' + self.getInstanceId(), toGrpcMasterInstance(self).toByteArray())
                .doOnCompleted(() -> this.ownMasterInstance = self)
                .doOnError(e -> logger.warn("Couldn't update own MasterInstance data in Zookeeper", e));
    }

    @Override
    public Observable<List<MasterInstance>> observeMasters() {
        return masterUpdatesObserver;
    }

    private void retrieveLeader() {
        try {
            curator
                    .sync()  // sync with ZK before reading
                    .inBackground(
                            curator
                                    .getData()
                                    .inBackground((client, event) -> {
                                        MasterDescription description = ObjectMappers.defaultMapper().readValue(event.getData(), MasterDescription.class);
                                        logger.info("New master retrieved: {}", description);
                                        latestLeader.set(description);
                                        leaderSubject.onNext(description);

                                    })
                                    .forPath(leaderPath)
                    )
                    .forPath(leaderPath);

        } catch (Exception e) {
            logger.error("Failed to retrieve updated master information: {}", e.getMessage(), e);
        }
    }

    private void retrieveAllMasters(CuratorFramework curator, TreeCacheEvent cacheEvent) {
        List<MasterInstance> updatedMasterList = new ArrayList<>();
        for (Map.Entry<String, ChildData> entry : masterMonitor.getCurrentChildren(masterPath).entrySet()) {
            parseMasterInstanceData(entry.getValue()).ifPresent(updatedMasterList::add);
        }
        masterUpdates.onNext(Collections.unmodifiableList(updatedMasterList));
    }

    private Optional<MasterInstance> parseMasterInstanceData(ChildData childData) {
        try {
            com.netflix.titus.grpc.protogen.MasterInstance grpcMasterInstance = com.netflix.titus.grpc.protogen.MasterInstance.parseFrom(childData.getData());
            return Optional.of(toCoreMasterInstance(grpcMasterInstance));
        } catch (Exception e) {
            titusRuntime.getCodeInvariants().unexpectedError("Found invalid MasterInstance protobuf error at: " + childData.getPath(), e);
            return Optional.empty();
        }
    }
}