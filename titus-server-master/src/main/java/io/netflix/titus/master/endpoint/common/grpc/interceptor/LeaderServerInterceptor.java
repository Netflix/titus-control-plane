/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint.common.grpc.interceptor;

import javax.inject.Inject;
import javax.inject.Singleton;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netflix.titus.master.cluster.LeaderActivator;

/**
 * Interceptor that that blocks all grpc calls until the system is ready.
 */
@Singleton
public final class LeaderServerInterceptor implements ServerInterceptor {

    private final LeaderActivator leaderActivator;

    @Inject
    public LeaderServerInterceptor(LeaderActivator leaderActivator) {
        this.leaderActivator = leaderActivator;
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                      ServerCallHandler<ReqT, RespT> next) {
        if (leaderActivator.isLeader()) {
            if (leaderActivator.isActivated()) {
                return next.startCall(call, headers);
            } else {
                call.close(Status.UNAVAILABLE.withDescription("Titus Master is initializing and not yet available."), new Metadata());
                return new ServerCall.Listener<ReqT>() {
                };
            }
        } else {
            call.close(Status.ABORTED.withDescription("Titus Master is not leader."), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }
    }
}
