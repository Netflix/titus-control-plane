/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.clustermembership;

import com.google.protobuf.Empty;
import com.netflix.titus.client.clustermembership.grpc.ClusterMembershipClient;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevisions;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc.ClusterMembershipServiceStub;
import com.netflix.titus.grpc.protogen.DeleteMemberLabelsRequest;
import com.netflix.titus.grpc.protogen.EnableMemberRequest;
import com.netflix.titus.grpc.protogen.MemberId;
import com.netflix.titus.grpc.protogen.UpdateMemberLabelsRequest;
import com.netflix.titus.runtime.endpoint.common.grpc.assistant.GrpcClientCallAssistant;
import com.netflix.titus.runtime.endpoint.common.grpc.assistant.GrpcClientCallAssistantFactory;
import io.grpc.ManagedChannel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultClusterMembershipClient implements ClusterMembershipClient {

    private final GrpcClientCallAssistant<ClusterMembershipServiceStub> assistant;
    private final ManagedChannel managedChannel;

    public DefaultClusterMembershipClient(GrpcClientCallAssistantFactory assistantFactory,
                                          ManagedChannel managedChannel) {
        this.managedChannel = managedChannel;
        this.assistant = assistantFactory.create(ClusterMembershipServiceGrpc.newStub(managedChannel));
    }

    @Override
    public Mono<ClusterMembershipRevisions> getMembers() {
        return assistant.asMono((stub, responseStream) -> stub.getMembers(Empty.getDefaultInstance(), responseStream));
    }

    @Override
    public Mono<ClusterMembershipRevision> getMember(MemberId request) {
        return assistant.asMono((stub, responseStream) -> stub.getMember(request, responseStream));
    }

    @Override
    public Mono<ClusterMembershipRevision> updateMemberLabels(UpdateMemberLabelsRequest request) {
        return assistant.asMono((stub, responseStream) -> stub.updateMemberLabels(request, responseStream));
    }

    @Override
    public Mono<ClusterMembershipRevision> deleteMemberLabels(DeleteMemberLabelsRequest request) {
        return assistant.asMono((stub, responseStream) -> stub.deleteMemberLabels(request, responseStream));
    }

    @Override
    public Mono<ClusterMembershipRevision> enableMember(EnableMemberRequest request) {
        return assistant.asMono((stub, responseStream) -> stub.enableMember(request, responseStream));
    }

    @Override
    public Mono<Void> stopBeingLeader() {
        return assistant.asMonoEmpty((stub, responseStream) -> stub.stopBeingLeader(Empty.getDefaultInstance(), responseStream));
    }

    @Override
    public Flux<ClusterMembershipEvent> events() {
        return assistant.asFlux((stub, responseStream) -> stub.events(Empty.getDefaultInstance(), responseStream));
    }

    @Override
    public void shutdown() {
        ExceptionExt.silent(managedChannel::shutdownNow);
    }
}
