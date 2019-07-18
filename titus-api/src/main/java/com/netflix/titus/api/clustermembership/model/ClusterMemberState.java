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

public enum ClusterMemberState {

    /**
     * Member instance is starting, and its operational state is not known yet.
     */
    Starting,

    /**
     * Member is inactive, and does not participate in the leader election process. Predefined reason codes in this state:
     * <ul>
     * <li>'outOfService' - the instance is healthy, but taken of out of service by an administrator</li>
     * <li>'unhealthy' - the instance is not healthy</li>
     * </ul>
     */
    Inactive,

    /**
     * Member is fully activated, and if does not participate in the leader election process, is ready to
     * take traffic.
     */
    Active
}
