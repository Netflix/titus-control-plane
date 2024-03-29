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

package com.netflix.titus.client.clustermembership.grpc;

import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevisions;
import com.netflix.titus.grpc.protogen.DeleteMemberLabelsRequest;
import com.netflix.titus.grpc.protogen.EnableMemberRequest;
import com.netflix.titus.grpc.protogen.MemberId;
import com.netflix.titus.grpc.protogen.UpdateMemberLabelsRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactor wrapper around the GRPC ClusterMembershipService documented at:
 * https://github.com/Netflix/titus-api-definitions/blob/master/src/main/proto/netflix/titus/titus_cluster_membership_api.proto.
 */
public interface ClusterMembershipClient {

    Mono<ClusterMembershipRevisions> getMembers();

    Mono<ClusterMembershipRevision> getMember(MemberId request);

    Mono<ClusterMembershipRevision> updateMemberLabels(UpdateMemberLabelsRequest request);

    Mono<ClusterMembershipRevision> deleteMemberLabels(DeleteMemberLabelsRequest request);

    Mono<ClusterMembershipRevision> enableMember(EnableMemberRequest request);

    Mono<Void> stopBeingLeader();

    Flux<ClusterMembershipEvent> events();

    void shutdown();
}
