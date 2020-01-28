/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.runtime.clustermembership.activation.LeaderActivationStatus;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;

/**
 * Interceptor that rejects incoming GRPC requests if the current node is not a leader.
 */
public class GrpcLeaderServerInterceptor implements ServerInterceptor {

    private final LeaderActivationStatus leaderActivationStatus;
    private final Set<MethodDescriptor> notProtectedMethods;

    public GrpcLeaderServerInterceptor(LeaderActivationStatus leaderActivationStatus,
                                       List<ServiceDescriptor> notProtectedServices) {
        this.leaderActivationStatus = leaderActivationStatus;
        this.notProtectedMethods = notProtectedServices.stream()
                .flatMap(s -> s.getMethods().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        MethodDescriptor<ReqT, RespT> methodDescriptor = call.getMethodDescriptor();
        if (notProtectedMethods.contains(methodDescriptor)) {
            return next.startCall(call, headers);
        }

        if (leaderActivationStatus.isActivatedLeader()) {
            return next.startCall(call, headers);
        } else {
            call.close(Status.UNAVAILABLE.withDescription("Not a leader or not ready yet."), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }
    }

    public static GrpcLeaderServerInterceptor clusterMembershipAllowed(LeaderActivationStatus leaderActivationStatus) {
        return new GrpcLeaderServerInterceptor(leaderActivationStatus, Collections.singletonList(ClusterMembershipServiceGrpc.getServiceDescriptor()));
    }
}
