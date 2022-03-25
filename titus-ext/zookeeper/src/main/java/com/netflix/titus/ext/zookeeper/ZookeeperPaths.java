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

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.curator.utils.ZKPaths;

/**
 * This class provides information about all Zookeeper paths used in Titus.
 */
@Singleton
public class ZookeeperPaths {

    public static final String ALL_MASTERS = "/allMasters";
    public static final String LEADER_ANNOUNCEMENT_NODE = "/leader";
    public static final String LEADER_ELECTION_NODE = "/hosts";

    private final String allMastersPath;
    private final String leaderElectionPath;
    private final String leaderAnnouncementPath;

    @Inject
    public ZookeeperPaths(ZookeeperConfiguration configuration) {
        String titusMasterPath = ZKPaths.makePath(configuration.getZkRoot(), "master");
        this.allMastersPath = ZKPaths.makePath(titusMasterPath, ALL_MASTERS);
        this.leaderElectionPath = ZKPaths.makePath(titusMasterPath, LEADER_ELECTION_NODE);
        this.leaderAnnouncementPath = ZKPaths.makePath(titusMasterPath, LEADER_ANNOUNCEMENT_NODE);
    }

    public String getAllMastersPath() {
        return allMastersPath;
    }

    public String getLeaderElectionPath() {
        return leaderElectionPath;
    }

    public String getLeaderAnnouncementPath() {
        return leaderAnnouncementPath;
    }

    @Override
    public String toString() {
        return "ZookeeperPaths{" +
                "allMastersPath='" + allMastersPath + '\'' +
                ", leaderElectionPath='" + leaderElectionPath + '\'' +
                ", leaderAnnouncementPath='" + leaderAnnouncementPath + '\'' +
                '}';
    }
}
