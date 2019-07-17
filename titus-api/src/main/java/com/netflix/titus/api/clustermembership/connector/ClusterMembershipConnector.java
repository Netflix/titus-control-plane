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

import java.util.function.UnaryOperator;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Connector to an external cluster membership orchestrator (Etcd, Zookeeper, etc).
 * <p>
 * {@link #joinLeaderElectionProcess(UnaryOperator)}, {@link #leaveLeaderElectionProcess(UnaryOperator)} and
 * {@link #updateSelf(UnaryOperator)} operations are serialized.
 */
public interface ClusterMembershipConnector {

    /**
     * Join the leader election process. This transactionally includes self {@link ClusterMember} update.
     * If leadership state is changed to the same value as the current one, return the current {@link ClusterMember} value,
     * and do nothing.
     */
    Mono<ClusterMember> joinLeaderElectionProcess(UnaryOperator<ClusterMember> selfUpdate);

    /**
     * Leave the leader election process. This transactionally includes self {@link ClusterMember} update.
     * If the current instance is an elected leader, return an error.
     * If leadership state is changed to the same value as the current one, return the current {@link ClusterMember} value,
     * and do nothing.
     */
    Mono<ClusterMember> leaveLeaderElectionProcess(UnaryOperator<ClusterMember> selfUpdate);

    /**
     * Store {@link ClusterMember} data associated with the given instance. If updates are executed concurrently, they
     * are serialized, and each request is provided with the latest version of the {@link ClusterMember}.
     * The {@link ClusterMemberLeadershipState} cannot be changed. If it is, the  update is rejected and an error is returned.
     */
    Mono<ClusterMember> updateSelf(UnaryOperator<ClusterMember> selfUpdate);

    /**
     * Requests the member that handles this request to stop being leader. If the given member
     * is not a leader, the request is ignored.
     */
    Mono<Void> stopBeingLeader();

    /**
     * Cluster membership change events. Changes originated from this instance via one of the methods above are
     * emitted after they are received by the external system.
     */
    Flux<ClusterMembershipEvent> events();
}
