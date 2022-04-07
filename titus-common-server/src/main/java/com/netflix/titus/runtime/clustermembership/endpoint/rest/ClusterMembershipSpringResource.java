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

package com.netflix.titus.runtime.clustermembership.endpoint.rest;

import java.time.Duration;

import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevisions;
import com.netflix.titus.grpc.protogen.DeleteMemberLabelsRequest;
import com.netflix.titus.grpc.protogen.EnableMemberRequest;
import com.netflix.titus.grpc.protogen.MemberId;
import com.netflix.titus.grpc.protogen.UpdateMemberLabelsRequest;
import com.netflix.titus.runtime.clustermembership.endpoint.grpc.GrpcClusterMembershipService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/v3/clustermembership")
public class ClusterMembershipSpringResource {

    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private final GrpcClusterMembershipService clusterMembershipService;

    public ClusterMembershipSpringResource(GrpcClusterMembershipService clusterMembershipService) {
        this.clusterMembershipService = clusterMembershipService;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/members", produces = "application/json")
    public ClusterMembershipRevisions getMembers() {
        return clusterMembershipService.getMembersInternal();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/members/{memberId}", produces = "application/json")
    public ClusterMembershipRevision getMember(@PathVariable("memberId") String memberId) {
        return clusterMembershipService.getMemberInternal(MemberId.newBuilder().setId(memberId).build());
    }

    @RequestMapping(method = RequestMethod.POST, path = "/members/{memberId}/labels", consumes = "application/json", produces = "application/json")
    public ClusterMembershipRevision updateMemberLabels(@PathVariable("memberId") String memberId,
                                                        @RequestBody UpdateMemberLabelsRequest request) {
        if (request.getMemberId().equals(memberId)) {
            throw new IllegalArgumentException("Member id in path and request body are different");
        }
        return clusterMembershipService.updateMemberLabelsInternal(request).block(TIMEOUT);
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/members/{memberId}/labels", consumes = "application/json", produces = "application/json")
    public ClusterMembershipRevision deleteMemberLabels(@PathVariable("memberId") String memberId,
                                                        @RequestBody DeleteMemberLabelsRequest request) {
        if (request.getMemberId().equals(memberId)) {
            throw new IllegalArgumentException("Member id in path and request body are different");
        }
        return clusterMembershipService.deleteMemberLabelsInternal(request).block(TIMEOUT);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/members/{memberId}/enable", consumes = "application/json", produces = "application/json")
    public ClusterMembershipRevision enableMember(@PathVariable("memberId") String memberId,
                                                  @RequestBody EnableMemberRequest request) {
        if (request.getMemberId().equals(memberId)) {
            throw new IllegalArgumentException("Member id in path and request body are different");
        }
        return clusterMembershipService.enableMemberInternal(request).block(TIMEOUT);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/members/{memberId}/stopBeingLeader")
    public void stopBeingLeader() {
        clusterMembershipService.stopBeingLeaderInternal().block(TIMEOUT);
    }
}
