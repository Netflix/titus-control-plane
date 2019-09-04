/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.clustermembership.model;

public enum ClusterMemberLeadershipState {

    /**
     * Member is not part of the leadership process.
     */
    Disabled,

    /**
     * Member is healthy, and participates in the leader election process, but is not the leader yet.
     */
    NonLeader,

    /**
     * Member is a current leader.
     */
    Leader,

    /**
     * Leadership state of a member is unknown.
     */
    Unknown
}
