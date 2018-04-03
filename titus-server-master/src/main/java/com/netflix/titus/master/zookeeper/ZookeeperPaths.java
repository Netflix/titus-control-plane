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

package com.netflix.titus.master.zookeeper;

import org.apache.curator.utils.ZKPaths;

/**
 * This class provides information about all Zookeeper paths used in Titus.
 */
public class ZookeeperPaths {

    public static final String LEADER_ANNOUNCEMENT_NODE = "/leader";
    public static final String LEADER_ELECTION_NODE = "/hosts";

    private final String titusMasterPath;
    private final String leaderElectionPath;
    private final String leaderAnnouncementPath;
    private final String mesosPath;

    public ZookeeperPaths(String zkRoot) {
        this.titusMasterPath = ZKPaths.makePath(zkRoot, "master");
        this.leaderElectionPath = ZKPaths.makePath(titusMasterPath, LEADER_ELECTION_NODE);
        this.leaderAnnouncementPath = ZKPaths.makePath(titusMasterPath, LEADER_ANNOUNCEMENT_NODE);
        this.mesosPath = ZKPaths.makePath(zkRoot, "/mesos");
    }

    public String getTitusMasterPath() {
        return titusMasterPath;
    }

    public String getLeaderElectionPath() {
        return leaderElectionPath;
    }

    public String getLeaderAnnouncementPath() {
        return leaderAnnouncementPath;
    }

    public String getMesosPath() {
        return mesosPath;
    }

    @Override
    public String toString() {
        return "ZookeeperPaths{" +
                "titusMasterPath='" + titusMasterPath + '\'' +
                ", leaderElectionPath='" + leaderElectionPath + '\'' +
                ", leaderAnnouncementPath='" + leaderAnnouncementPath + '\'' +
                ", mesosPath='" + mesosPath + '\'' +
                '}';
    }
}
