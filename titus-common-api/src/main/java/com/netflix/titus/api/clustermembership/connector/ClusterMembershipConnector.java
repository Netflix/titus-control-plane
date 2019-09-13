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

package com.netflix.titus.api.clustermembership.connector;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Connector to an external cluster membership orchestrator (Etcd, Zookeeper, Kubernetes, etc).
 */
public interface ClusterMembershipConnector {

    /**
     * Returns cluster member data for the local member.
     */
    ClusterMembershipRevision<ClusterMember> getLocalClusterMemberRevision();

    /**
     * Returns all known siblings of the given cluster member.
     */
    Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings();

    /**
     * Returns the leadership state of the local member.
     */
    ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadershipRevision();

    /**
     * Returns current leader or {@link Optional#empty()} if there is no leader.
     */
    Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findCurrentLeader();

    /**
     * Store {@link ClusterMember} data associated with the given instance. The result {@link Mono} completes when
     * the request data are persisted in the external store.
     *
     * <h1>Event ordering</h1>
     * Concurrent calls to this method are serialized. After the change is recorded by the connector, a corresponding
     * event is first emitted by {@link #membershipChangeEvents()}, and only after the request is completed, and the
     * identical revision data are emitted in response.
     *
     * @return cluster member revision created by this update operation
     */
    Mono<ClusterMembershipRevision<ClusterMember>> register(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate);

    /**
     * Remove the local member from the membership group. If the member is part of the leadership process the request
     * is rejected.
     */
    Mono<ClusterMembershipRevision<ClusterMember>> unregister(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate);

    /**
     * Join leader election process. Only allowed for registered members.
     */
    Mono<Void> joinLeadershipGroup();

    /**
     * Request the local member to leave the leader election process. The method completes when the leadership abort
     * is completed. This means that by the time it completes, another cluster member can be elected a new leader and
     * take over the traffic. It is important this this method is only called after the local node stops taking traffic,
     * and synchronizes all state to external stores.
     *
     * <h1>Event ordering</h1>
     * Follow the same rules as {@link #register(Function)} (see above).
     *
     * @param onlyNonLeader if set to true, the leader election process is terminated only if the local member is not an elected leader
     * @return true if the local member left the leader election process
     */
    Mono<Boolean> leaveLeadershipGroup(boolean onlyNonLeader);

    /**
     * Cluster membership change events. The event stream includes all members both local and siblings.
     * Emits snapshot data on subscription followed by update events.
     */
    Flux<ClusterMembershipEvent> membershipChangeEvents();
}
