/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.testkit.rx;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentChangeEvent;
import com.netflix.titus.grpc.protogen.AgentInstance;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentInstances;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AgentQuery;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import rx.Completable;
import rx.Observable;

public class RxGrpcAgentManagementService {

    private final ManagedChannel channel;

    public RxGrpcAgentManagementService(ManagedChannel channel) {
        this.channel = channel;
    }

    public Observable<AgentInstanceGroups> getInstanceGroups() {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(AgentManagementServiceGrpc.METHOD_GET_INSTANCE_GROUPS, CallOptions.DEFAULT),
                    Empty.getDefaultInstance()
            ).subscribe(subscriber);
        });
    }

    public Observable<AgentInstanceGroup> getInstanceGroup(String id) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(AgentManagementServiceGrpc.METHOD_GET_INSTANCE_GROUP, CallOptions.DEFAULT),
                    Id.newBuilder().setId(id).build()
            ).subscribe(subscriber);
        });
    }

    public Observable<AgentInstance> getAgentInstance(String id) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(AgentManagementServiceGrpc.METHOD_GET_AGENT_INSTANCE, CallOptions.DEFAULT),
                    Id.newBuilder().setId(id).build()
            ).subscribe(subscriber);
        });
    }

    public Observable<AgentInstances> findAgentInstances(AgentQuery query) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(AgentManagementServiceGrpc.METHOD_FIND_AGENT_INSTANCES, CallOptions.DEFAULT),
                    query
            ).subscribe(subscriber);
        });
    }

    public Completable updateLifecycle(InstanceGroupLifecycleStateUpdate lifecycleStateUpdate) {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(AgentManagementServiceGrpc.METHOD_UPDATE_INSTANCE_GROUP_LIFECYCLE_STATE, CallOptions.DEFAULT),
                    lifecycleStateUpdate
            ).subscribe(subscriber);
        }).toCompletable();
    }

    public Observable<AgentChangeEvent> observeAgents() {
        return Observable.unsafeCreate(subscriber -> {
            ObservableClientCall.create(
                    channel.newCall(AgentManagementServiceGrpc.METHOD_OBSERVE_AGENTS, CallOptions.DEFAULT),
                    Empty.getDefaultInstance()
            ).subscribe(subscriber);
        });
    }
}
