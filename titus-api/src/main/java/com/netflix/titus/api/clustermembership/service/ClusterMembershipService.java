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

package com.netflix.titus.api.clustermembership.service;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClusterMembershipService {

    /**
     * Returns cluster member data for the local member.
     */
    ClusterMembershipRevision<ClusterMember> getLocalClusterMember();

    /**
     * Returns all known siblings of the given cluster member.
     */
    Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings();

    /**
     * Returns the leadership state of the local member.
     */
    ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadership();

    /**
     * Find leader.
     */
    Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findLeader();

    /**
     * Updates {@link ClusterMember} data associated with the given instance.
     */
    Mono<ClusterMembershipRevision<ClusterMember>> updateSelf(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> memberUpdate);

    /**
     * Requests the member that handles this request to stop being leader. If the given member
     * is not a leader, the request is ignored.
     */
    Mono<Void> stopBeingLeader();

    /**
     * Cluster membership change events.
     */
    Flux<ClusterMembershipEvent> events(boolean snapshot);
}
