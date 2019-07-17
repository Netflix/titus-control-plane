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

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClusterMembershipService {

    /**
     * Get all known cluster members.
     */
    List<ClusterMember> getMembers();

    /**
     * Get member with the given id.
     */
    Optional<ClusterMember> findMember(String memberId);

    /**
     * Updates {@link ClusterMember} data associated with the given instance.
     */
    Mono<ClusterMember> updateSelf(UnaryOperator<ClusterMember> member);

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
