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

package com.netflix.titus.master.supervisor.endpoint.grpc;

import java.util.stream.Collectors;

import com.netflix.titus.api.supervisor.model.MasterInstance;
import com.netflix.titus.api.supervisor.model.MasterState;
import com.netflix.titus.api.supervisor.model.MasterStatus;
import com.netflix.titus.api.supervisor.model.event.MasterInstanceRemovedEvent;
import com.netflix.titus.api.supervisor.model.event.MasterInstanceUpdateEvent;
import com.netflix.titus.api.supervisor.model.event.SupervisorEvent;

public class SupervisorGrpcModelConverters {

    public static MasterInstance toCoreMasterInstance(com.netflix.titus.grpc.protogen.MasterInstance grpcMasterInstance) {
        return MasterInstance.newBuilder()
                .withInstanceId(grpcMasterInstance.getInstanceId())
                .withIpAddress(grpcMasterInstance.getIpAddress())
                .withStatus(toCoreStatus(grpcMasterInstance.getStatus()))
                .withStatusHistory(grpcMasterInstance.getStatusHistoryList().stream().map(SupervisorGrpcModelConverters::toCoreStatus).collect(Collectors.toList()))
                .build();
    }

    public static com.netflix.titus.grpc.protogen.MasterInstance toGrpcMasterInstance(MasterInstance masterInstance) {
        return com.netflix.titus.grpc.protogen.MasterInstance.newBuilder()
                .setInstanceId(masterInstance.getInstanceId())
                .setIpAddress(masterInstance.getIpAddress())
                .setStatus(toGrpcStatus(masterInstance.getStatus()))
                .addAllStatusHistory(masterInstance.getStatusHistory().stream().map(SupervisorGrpcModelConverters::toGrpcStatus).collect(Collectors.toList()))
                .build();
    }

    public static MasterStatus toCoreStatus(com.netflix.titus.grpc.protogen.MasterStatus grpcStatus) {
        return MasterStatus.newBuilder()
                .withState(toCoreState(grpcStatus.getState()))
                .withReasonCode(grpcStatus.getReasonCode())
                .withReasonMessage(grpcStatus.getReasonMessage())
                .withTimestamp(grpcStatus.getTimestamp())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.MasterStatus toGrpcStatus(MasterStatus masterStatus) {
        return com.netflix.titus.grpc.protogen.MasterStatus.newBuilder()
                .setState(toGrpcState(masterStatus.getState()))
                .setTimestamp(masterStatus.getTimestamp())
                .setReasonCode(masterStatus.getReasonCode())
                .setReasonMessage(masterStatus.getReasonMessage())
                .build();
    }

    public static MasterState toCoreState(com.netflix.titus.grpc.protogen.MasterStatus.MasterState grpcState) {
        switch (grpcState) {
            case Starting:
                return MasterState.Starting;
            case Inactive:
                return MasterState.Inactive;
            case NonLeader:
                return MasterState.NonLeader;
            case LeaderActivating:
                return MasterState.LeaderActivating;
            case LeaderActivated:
                return MasterState.LeaderActivated;
        }
        throw new IllegalArgumentException("Unrecognized GRPC MasterState state: " + grpcState);
    }

    public static com.netflix.titus.grpc.protogen.MasterStatus.MasterState toGrpcState(MasterState state) {
        switch (state) {
            case Starting:
                return com.netflix.titus.grpc.protogen.MasterStatus.MasterState.Starting;
            case Inactive:
                return com.netflix.titus.grpc.protogen.MasterStatus.MasterState.Inactive;
            case NonLeader:
                return com.netflix.titus.grpc.protogen.MasterStatus.MasterState.NonLeader;
            case LeaderActivating:
                return com.netflix.titus.grpc.protogen.MasterStatus.MasterState.LeaderActivating;
            case LeaderActivated:
                return com.netflix.titus.grpc.protogen.MasterStatus.MasterState.LeaderActivated;
        }
        throw new IllegalArgumentException("Unrecognized core MasterState state: " + state);
    }

    public static com.netflix.titus.grpc.protogen.SupervisorEvent toGrpcEvent(SupervisorEvent coreEvent) {
        if (coreEvent instanceof MasterInstanceUpdateEvent) {
            return com.netflix.titus.grpc.protogen.SupervisorEvent.newBuilder()
                    .setMasterInstanceUpdate(com.netflix.titus.grpc.protogen.SupervisorEvent.MasterInstanceUpdate.newBuilder()
                            .setInstance(toGrpcMasterInstance(((MasterInstanceUpdateEvent) coreEvent).getMasterInstance()))
                    )
                    .build();
        }
        if (coreEvent instanceof MasterInstanceRemovedEvent) {
            return com.netflix.titus.grpc.protogen.SupervisorEvent.newBuilder()
                    .setMasterInstanceRemoved(com.netflix.titus.grpc.protogen.SupervisorEvent.MasterInstanceRemoved.newBuilder()
                            .setInstanceId(((MasterInstanceRemovedEvent) coreEvent).getMasterInstance().getInstanceId())
                    )
                    .build();
        }
        throw new IllegalArgumentException("Unrecognized supervisor event: " + coreEvent);
    }
}
