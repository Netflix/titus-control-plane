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

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.ext.zookeeper.ZookeeperPaths;
import com.netflix.titus.ext.zookeeper.connector.CuratorService;
import com.netflix.titus.ext.zookeeper.connector.CuratorUtils;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.service.LeaderActivator;
import com.netflix.titus.api.supervisor.service.LeaderElector;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;

import static org.apache.zookeeper.KeeperException.Code.OK;

@Singleton
public class ZookeeperLeaderElector implements LeaderElector {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperLeaderElector.class);

    private final LeaderActivator leaderActivator;
    private final ZookeeperPaths zookeeperPaths;
    private final TitusRuntime titusRuntime;
    private final ObjectMapper jsonMapper;
    private final MasterDescription masterDescription;
    private final CuratorFramework curator;
    // The path where a selected leader announces itself.
    private final String leaderPath;

    private final BehaviorSubject<MasterState> electionSubject = BehaviorSubject.create();
    private final Observable<MasterState> electionObserver = ObservableExt.protectFromMissingExceptionHandlers(electionSubject, logger);

    private final AtomicReference<LeaderElectionProcess> leaderElectionProcessRef = new AtomicReference<>();

    @Inject
    public ZookeeperLeaderElector(LeaderActivator leaderActivator,
                                  CuratorService curatorService,
                                  ZookeeperPaths zookeeperPaths,
                                  MasterDescription masterDescription,
                                  TitusRuntime titusRuntime) {
        this.leaderActivator = leaderActivator;
        this.zookeeperPaths = zookeeperPaths;
        this.titusRuntime = titusRuntime;
        this.jsonMapper = ObjectMappers.defaultMapper();
        this.masterDescription = masterDescription;
        this.curator = curatorService.getCurator();
        this.leaderPath = zookeeperPaths.getLeaderAnnouncementPath();

        initialize();
    }

    private void initialize() {
        CuratorUtils.createPathIfNotExist(curator, leaderPath, true);
    }

    @PreDestroy
    public void shutdown() {
        LeaderElectionProcess process = leaderElectionProcessRef.getAndSet(null);
        if (process != null) {
            process.close();
        }
    }

    @Override
    public boolean join() {
        synchronized (leaderElectionProcessRef) {
            if (leaderElectionProcessRef.get() == null) {
                leaderElectionProcessRef.set(new LeaderElectionProcess());
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean leaveIfNotLeader() {
        synchronized (leaderElectionProcessRef) {
            LeaderElectionProcess process = leaderElectionProcessRef.get();
            if (process == null) {
                return false;
            }
            if (process.leaveIfNotLeader()) {
                leaderElectionProcessRef.set(null);
                return true;
            }
        }
        return false;
    }

    @Override
    public Observable<MasterState> awaitElection() {
        return electionObserver;
    }


    private class LeaderElectionProcess {

        private final LeaderLatch leaderLatch;

        private volatile boolean leaderFlag;
        private volatile boolean closed;

        private LeaderElectionProcess() {
            this.leaderLatch = createNewLeaderLatch(zookeeperPaths.getLeaderElectionPath());
            try {
                leaderLatch.start();
            } catch (Exception e) {
                String errorMessage = "Failed to create a leader elector for master: " + e.getMessage();

                titusRuntime.getSystemLogService().submit(SystemLogEvent.newBuilder()
                        .withComponent(LeaderActivator.COMPONENT)
                        .withCategory(SystemLogEvent.Category.Transient)
                        .withPriority(SystemLogEvent.Priority.Fatal)
                        .withMessage(errorMessage)
                        .withContext(Collections.singletonMap("error", e.getMessage()))
                        .build()
                );

                throw new IllegalStateException(errorMessage, e);
            }
        }

        private boolean leaveIfNotLeader() {
            synchronized (this) {
                if (leaderFlag) {
                    return false;
                }

                // Not a leader yet, so it is safe to leave the leader election process.
                close();
            }

            if (leaderActivator.isLeader()) {
                logger.error("Unexpected to be a leader. Terminating the JVM process");
                System.exit(-1);
            }

            return true;
        }

        private void close() {
            closed = true;
            try {
                leaderLatch.close();
            } catch (Exception e) {
                logger.warn("Error when leaving the leader election process", e);
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
                    }, Executors.newSingleThreadExecutor(new DefaultThreadFactory("LeaderLatchListener-%s")));

            return newLeaderLatch;
        }

        private void announceLeader() {
            try {
                logger.info("Announcing leader");
                byte[] masterDescriptionBytes = jsonMapper.writeValueAsBytes(masterDescription);

                // There is no need to lock anything because we ensure only leader will write to the leader path
                curator
                        .setData()
                        .inBackground((client, event) -> {
                            if (event.getResultCode() == OK.intValue()) {
                                synchronized (LeaderElectionProcess.this) {
                                    terminateIfClosed();

                                    leaderFlag = true;
                                    electionSubject.onNext(MasterState.LeaderActivating);
                                    leaderActivator.becomeLeader();
                                    electionSubject.onNext(MasterState.LeaderActivated);
                                }
                            } else {
                                logger.warn("Failed to elect leader from path {} with event {}", leaderPath, event);
                            }
                        }).forPath(leaderPath, masterDescriptionBytes);
            } catch (Exception e) {
                throw new RuntimeException("Failed to announce leader: " + e.getMessage(), e);
            }
        }

        private void terminateIfClosed() {
            if (closed) {
                logger.error("Received leader activation request after initiating withdrawal from the leader election process. Terminating the JVM process");
                System.exit(-1);
            }
        }
    }
}
