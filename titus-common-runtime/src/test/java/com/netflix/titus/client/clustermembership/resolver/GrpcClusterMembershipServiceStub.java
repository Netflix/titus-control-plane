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

package com.netflix.titus.client.clustermembership.resolver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc.ClusterMembershipServiceImplBase;
import io.grpc.stub.StreamObserver;

class GrpcClusterMembershipServiceStub extends ClusterMembershipServiceImplBase {

    private final ConcurrentMap<String, ClusterMembershipRevision> memberRevisions = new ConcurrentHashMap<>();

    private volatile StreamObserver<ClusterMembershipEvent> currentEventResponseObserver;

    @Override
    public void events(Empty request, StreamObserver<ClusterMembershipEvent> responseObserver) {
        this.currentEventResponseObserver = responseObserver;
        currentEventResponseObserver.onNext(ClusterMembershipEvent.newBuilder()
                .setSnapshot(ClusterMembershipEvent.Snapshot.newBuilder()
                        .addAllRevisions(memberRevisions.values())
                )
                .build()
        );
    }

    void addMember(ClusterMembershipRevision memberRevision) {
        memberRevisions.put(memberRevision.getCurrent().getMemberId(), memberRevision);
        if (currentEventResponseObserver != null) {
            currentEventResponseObserver.onNext(ClusterMembershipEvent.newBuilder()
                    .setMemberUpdated(ClusterMembershipEvent.MemberUpdated.newBuilder()
                            .setRevision(memberRevision)
                    )
                    .build()
            );
        }
    }

    void removeMember(ClusterMembershipRevision memberRevision) {
        memberRevisions.remove(memberRevision.getCurrent().getMemberId());
        if (currentEventResponseObserver != null) {
            currentEventResponseObserver.onNext(ClusterMembershipEvent.newBuilder()
                    .setMemberRemoved(ClusterMembershipEvent.MemberRemoved.newBuilder()
                            .setRevision(memberRevision)
                    )
                    .build()
            );
        }
    }

    void breakConnection() {
        StreamObserver<ClusterMembershipEvent> observerToBreak = currentEventResponseObserver;
        currentEventResponseObserver = null;
        observerToBreak.onError(new RuntimeException("Simulated connection error"));
    }

    void completeEventStream() {
        StreamObserver<ClusterMembershipEvent> observerToBreak = currentEventResponseObserver;
        currentEventResponseObserver = null;
        observerToBreak.onCompleted();
    }
}
