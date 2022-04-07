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

package com.netflix.titus.runtime.clustermembership.endpoint.grpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipSnapshotEvent;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.api.clustermembership.service.ClusterMembershipServiceException;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevisions;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.grpc.protogen.DeleteMemberLabelsRequest;
import com.netflix.titus.grpc.protogen.EnableMemberRequest;
import com.netflix.titus.grpc.protogen.MemberId;
import com.netflix.titus.grpc.protogen.UpdateMemberLabelsRequest;
import com.netflix.titus.runtime.endpoint.common.grpc.assistant.GrpcServerCallAssistant;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.client.clustermembership.grpc.ClusterMembershipGrpcConverters.toGrpcClusterMembershipRevision;
import static com.netflix.titus.runtime.clustermembership.endpoint.grpc.MemberDataMixer.NO_LEADER_ID;

@Singleton
public class GrpcClusterMembershipService extends ClusterMembershipServiceGrpc.ClusterMembershipServiceImplBase {

    private final String localMemberId;
    private final ClusterMembershipService service;
    private final GrpcServerCallAssistant assistant;
    private final Clock clock;

    @Inject
    public GrpcClusterMembershipService(ClusterMembershipService service,
                                        GrpcServerCallAssistant assistant,
                                        TitusRuntime titusRuntime) {
        this.localMemberId = service.getLocalLeadership().getCurrent().getMemberId();
        this.service = service;
        this.assistant = assistant;
        this.clock = titusRuntime.getClock();
    }

    /**
     * Get all known cluster members.
     */
    @Override
    public void getMembers(Empty request, StreamObserver<ClusterMembershipRevisions> responseObserver) {
        assistant.callDirect(responseObserver, context -> getMembersInternal());
    }

    public ClusterMembershipRevisions getMembersInternal() {
        List<ClusterMembershipRevision> grpcRevisions = new ArrayList<>();
        grpcRevisions.add(toGrpcClusterMembershipRevision(service.getLocalClusterMember(), isLocalLeader()));

        String leaderId = getLeaderId();
        for (com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision<ClusterMember> sibling : service.getClusterMemberSiblings().values()) {
            grpcRevisions.add(toGrpcClusterMembershipRevision(sibling, sibling.getCurrent().getMemberId().equals(leaderId)));
        }

        return ClusterMembershipRevisions.newBuilder().addAllRevisions(grpcRevisions).build();
    }

    /**
     * Get member with the given id.
     */
    @Override
    public void getMember(MemberId request, StreamObserver<ClusterMembershipRevision> responseObserver) {
        assistant.callDirect(responseObserver, context -> getMemberInternal(request));
    }

    public ClusterMembershipRevision getMemberInternal(MemberId request) {
        String memberId = request.getId();

        if (service.getLocalClusterMember().getCurrent().getMemberId().equals(memberId)) {
            return toGrpcClusterMembershipRevision(service.getLocalClusterMember(), isLocalLeader());
        }

        com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision<ClusterMember> sibling = service.getClusterMemberSiblings().get(memberId);
        if (sibling == null) {
            throw ClusterMembershipServiceException.memberNotFound(memberId);
        }
        return toGrpcClusterMembershipRevision(sibling, getLeaderId().equals(memberId));
    }

    /**
     * Adds all labels from the request object to the target member. Labels that exist are
     * overridden. Returns the updated object.
     */
    @Override
    public void updateMemberLabels(UpdateMemberLabelsRequest request, StreamObserver<ClusterMembershipRevision> responseObserver) {
        assistant.callMono(responseObserver, context -> updateMemberLabelsInternal(request));
    }

    public Mono<ClusterMembershipRevision> updateMemberLabelsInternal(UpdateMemberLabelsRequest request) {
        if (!request.getMemberId().equals(localMemberId)) {
            return Mono.error(ClusterMembershipServiceException.localOnly(request.getMemberId()));
        }
        if (request.getLabelsMap().isEmpty()) {
            return Mono.fromCallable(() -> toGrpcClusterMembershipRevision(service.getLocalClusterMember(), isLocalLeader()));
        }
        return service.updateSelf(current ->
                com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision.<ClusterMember>newBuilder()
                        .withCurrent(current.toBuilder()
                                .withLabels(CollectionsExt.merge(current.getLabels(), request.getLabelsMap()))
                                .build()
                        )
                        .withCode("updated")
                        .withMessage("Added labels: " + request.getLabelsMap())
                        .withTimestamp(clock.wallTime())
                        .build()
        ).map(c -> toGrpcClusterMembershipRevision(c, isLocalLeader()));
    }

    /**
     * Removes all specified labels from the target object. Labels that do not exist are ignored.
     * Returns the updated object.
     */
    @Override
    public void deleteMemberLabels(DeleteMemberLabelsRequest request, StreamObserver<ClusterMembershipRevision> responseObserver) {
        assistant.callMono(responseObserver, context -> deleteMemberLabelsInternal(request));
    }

    public Mono<ClusterMembershipRevision> deleteMemberLabelsInternal(DeleteMemberLabelsRequest request) {
        if (!request.getMemberId().equals(localMemberId)) {
            return Mono.error(ClusterMembershipServiceException.localOnly(request.getMemberId()));
        }
        if (request.getKeysList().isEmpty()) {
            return Mono.fromCallable(() -> toGrpcClusterMembershipRevision(service.getLocalClusterMember(), isLocalLeader()));
        }
        return service.updateSelf(current ->
                com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision.<ClusterMember>newBuilder()
                        .withCurrent(current.toBuilder()
                                .withLabels(CollectionsExt.copyAndRemove(current.getLabels(), request.getKeysList()))
                                .build()
                        )
                        .withCode("updated")
                        .withMessage("Removed labels: " + request.getKeysList())
                        .withTimestamp(clock.wallTime())
                        .build()
        ).map(c -> toGrpcClusterMembershipRevision(c, isLocalLeader()));
    }

    /**
     * Enable or disable a member.
     */
    @Override
    public void enableMember(EnableMemberRequest request, StreamObserver<ClusterMembershipRevision> responseObserver) {
        assistant.callMono(responseObserver, context -> enableMemberInternal(request));
    }

    public Mono<ClusterMembershipRevision> enableMemberInternal(EnableMemberRequest request) {
        if (!request.getMemberId().equals(localMemberId)) {
            return Mono.error(ClusterMembershipServiceException.localOnly(request.getMemberId()));
        }
        return service.updateSelf(current ->
                com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision.<ClusterMember>newBuilder()
                        .withCurrent(current.toBuilder()
                                .withEnabled(request.getEnabled())
                                .build()
                        )
                        .withCode(request.getEnabled() ? "enabled" : "disabled")
                        .withMessage("Changed enabled status to: " + request.getEnabled())
                        .withTimestamp(clock.wallTime())
                        .build()
        ).map(c -> toGrpcClusterMembershipRevision(c, isLocalLeader()));
    }

    /**
     * Requests the member that handles this request to stop being leader. If the given member
     * is not a leader, the request is ignored.
     */
    @Override
    public void stopBeingLeader(Empty request, StreamObserver<Empty> responseObserver) {
        assistant.callMonoEmpty(responseObserver, context -> stopBeingLeaderInternal());
    }

    public Mono<Void> stopBeingLeaderInternal() {
        return service.stopBeingLeader();
    }

    /**
     * Event stream.
     */
    @Override
    public void events(Empty request, StreamObserver<ClusterMembershipEvent> responseObserver) {
        assistant.callFlux(responseObserver, context -> Flux.defer(() -> {
            AtomicReference<MemberDataMixer> leaderRef = new AtomicReference<>();
            return service.events().flatMapIterable(event -> {
                MemberDataMixer data = leaderRef.get();
                if (data == null) {
                    Preconditions.checkArgument(
                            event instanceof ClusterMembershipSnapshotEvent,
                            "Expected ClusterMembershipSnapshotEvent as the first event"
                    );
                    ClusterMembershipSnapshotEvent snapshotEvent = (ClusterMembershipSnapshotEvent) event;
                    data = new MemberDataMixer(snapshotEvent.getClusterMemberRevisions(), snapshotEvent.getLeader());
                    leaderRef.set(data);
                    return Collections.singletonList(data.toGrpcSnapshot());
                }
                return data.process(event);
            });
        }));
    }

    private String getLeaderId() {
        return service.findLeader().map(l -> l.getCurrent().getMemberId()).orElse(NO_LEADER_ID);
    }

    private boolean isLocalLeader() {
        return service.getLocalLeadership().getCurrent().getLeadershipState() == ClusterMemberLeadershipState.Leader;
    }
}
