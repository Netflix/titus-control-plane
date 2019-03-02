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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.supervisor.service.LeaderActivator;
import com.netflix.titus.api.supervisor.service.MasterDescription;
import com.netflix.titus.api.supervisor.service.MasterMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 12/09/2014
 * This is being done as a 'big hammer' kind of workaround for a ZooKeeper error we encountered
 * for leader election. We have two callback routes from ZooKeeper: LeaderLatch and NodeCache.
 * Master uses LeaderLatch to be notified of when it needs to become leader and when to stop being one.
 * The node cache is used by clients to be notified of Leader changes. We make sure that the two match.
 * That is, we get node cache notifications and make sure that its view of whether or not we are the
 * leader matches LeaderLatch's view. If not, we panic and exit. This was observed to be inconsistent in
 * prod at least once.
 * We publish public hostname in master description, that's how we will know if ZK thinks we are the leader.
 */
@Singleton
public class ZkLeaderVerificator {

    private static final Logger logger = LoggerFactory.getLogger(ZkLeaderVerificator.class);

    private final MasterMonitor masterMonitor;
    private final LeaderActivator leaderActivator;

    @Inject
    public ZkLeaderVerificator(MasterMonitor masterMonitor, LeaderActivator leaderActivator) {
        this.masterMonitor = masterMonitor;
        this.leaderActivator = leaderActivator;
    }

    @PostConstruct
    void setupZKLeaderVerification() {
        final String myHostname = System.getenv("EC2_PUBLIC_HOSTNAME");
        final String myLocalIP = System.getenv("EC2_LOCAL_IPV4");
        if (myHostname == null || myHostname.isEmpty()) {
            logger.warn("Did not find public hostname variable, OK if not running cloud");
            return;
        }
        if (myLocalIP == null || myLocalIP.isEmpty()) {
            logger.warn("Did not find local IP variable, OK if not running cloud");
            return;
        }
        logger.info("Setting up ZK leader verification with myHostname=" + myHostname + ", localIP=" + myLocalIP);
        long delay = 20;
        final AtomicReference<MasterDescription> ref = new AtomicReference<>();

        masterMonitor.getLeaderObservable()
                .doOnNext(ref::set)
                .subscribe();
        final AtomicInteger falseCount = new AtomicInteger(0);
        final int MAX_FALSE_COUNTS = 10;
        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        boolean foundFault = false;
                        try {
                            if (leaderActivator.isLeader()) {
                                logger.info("I'm leader, masterDescription=" + ref.get());
                                if (ref.get() != null && !myHostname.equals(ref.get().getHostname()) && !myLocalIP.equals(ref.get().getHostname())) {
                                    foundFault = true;
                                    logger.warn("ZK says leader is " + ref.get().getHostname() + ", not us (" + myHostname + ")");
                                    if (falseCount.incrementAndGet() > MAX_FALSE_COUNTS) {
                                        logger.error("Too many attempts failed to verify ZK leader status, exiting!");
                                        System.exit(5);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Error verifying leader status: " + e.getMessage(), e);
                        }
                        if (!foundFault) {
                            falseCount.set(0);
                        }
                    }
                },
                delay, delay, TimeUnit.SECONDS);
    }
}
