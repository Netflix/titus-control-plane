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

package com.netflix.titus.client.clustermembership.resolver;

import com.google.protobuf.Empty;
import com.netflix.titus.client.clustermembership.grpc.ClusterMembershipClient;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevisions;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc.ClusterMembershipServiceStub;
import com.netflix.titus.grpc.protogen.DeleteMemberLabelsRequest;
import com.netflix.titus.grpc.protogen.EnableMemberRequest;
import com.netflix.titus.grpc.protogen.MemberId;
import com.netflix.titus.grpc.protogen.UpdateMemberLabelsRequest;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/**
 * FIXME We need this implementation for testing as DefaultClusterMembershipClient is located in titus-common-server package.
 * To resolve this issue we have to move CallMetadata classes to titus-common-client.
 */
class SimpleClusterMembershipClient implements ClusterMembershipClient {

    private final ManagedChannel channel;
    private final ClusterMembershipServiceStub stub;

    public SimpleClusterMembershipClient(ManagedChannel channel) {
        this.channel = channel;
        this.stub = ClusterMembershipServiceGrpc.newStub(channel);
    }

    @Override
    public Mono<ClusterMembershipRevisions> getMembers() {
        return Mono.create(sink -> stub.getMembers(Empty.getDefaultInstance(), connectSink(sink)));
    }

    @Override
    public Mono<ClusterMembershipRevision> getMember(MemberId request) {
        return Mono.create(sink -> stub.getMember(request, connectSink(sink)));
    }

    @Override
    public Mono<ClusterMembershipRevision> updateMemberLabels(UpdateMemberLabelsRequest request) {
        return Mono.create(sink -> stub.updateMemberLabels(request, connectSink(sink)));
    }

    @Override
    public Mono<ClusterMembershipRevision> deleteMemberLabels(DeleteMemberLabelsRequest request) {
        return Mono.create(sink -> stub.deleteMemberLabels(request, connectSink(sink)));
    }

    @Override
    public Mono<ClusterMembershipRevision> enableMember(EnableMemberRequest request) {
        return Mono.create(sink -> stub.enableMember(request, connectSink(sink)));
    }

    @Override
    public Mono<Void> stopBeingLeader() {
        return Mono.<Empty>create(sink -> stub.stopBeingLeader(Empty.getDefaultInstance(), connectSink(sink)))
                .ignoreElement()
                .cast(Void.class);
    }

    @Override
    public Flux<ClusterMembershipEvent> events() {
        return Flux.create(sink -> {
            stub.events(Empty.getDefaultInstance(), new StreamObserver<ClusterMembershipEvent>() {
                @Override
                public void onNext(ClusterMembershipEvent value) {
                    sink.next(value);
                }

                @Override
                public void onError(Throwable t) {
                    sink.error(t);
                }

                @Override
                public void onCompleted() {
                    sink.complete();
                }
            });
        });
    }

    @Override
    public void shutdown() {
        channel.shutdownNow();
    }

    private <T> StreamObserver<T> connectSink(MonoSink<T> sink) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(T value) {
                sink.success(value);
            }

            @Override
            public void onError(Throwable t) {
                sink.error(t);
            }

            @Override
            public void onCompleted() {
                sink.success();
            }
        };
    }
}
