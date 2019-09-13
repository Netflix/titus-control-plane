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

import java.util.Optional;
import java.util.function.Function;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipServiceException;
import io.grpc.Status;

public class ClusterMembershipGrpcExceptionMapper implements Function<Throwable, Optional<Status>> {

    private static final ClusterMembershipGrpcExceptionMapper INSTANCE = new ClusterMembershipGrpcExceptionMapper();

    @Override
    public Optional<Status> apply(Throwable throwable) {
        if (!(throwable instanceof ClusterMembershipServiceException)) {
            return Optional.empty();
        }
        ClusterMembershipServiceException serviceException = (ClusterMembershipServiceException) throwable;
        switch (serviceException.getErrorCode()) {
            case BadSelfUpdate:
            case LocalMemberOnly:
                return Optional.of(Status.FAILED_PRECONDITION);
            case Internal:
                return Optional.of(Status.INTERNAL);
            case MemberNotFound:
                return Optional.of(Status.NOT_FOUND);
        }
        return Optional.empty();
    }

    public static ClusterMembershipGrpcExceptionMapper getInstance() {
        return INSTANCE;
    }
}
