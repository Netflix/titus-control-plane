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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.util.stream.Collectors;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;

public class CommonGrpcModelConverters2 {

    public static CallMetadata toCallMetadata(com.netflix.titus.grpc.protogen.CallMetadata grpcCallContext) {
        return CallMetadata.newBuilder()
                .withCallerId(grpcCallContext.getCallerId())
                .withCallReason(grpcCallContext.getCallReason())
                .withCallPath(grpcCallContext.getCallPathList())
                .withCallers(grpcCallContext.getCallersList().stream().map(CommonGrpcModelConverters2::toCoreCaller).collect(Collectors.toList()))
                .withDebug(grpcCallContext.getDebug())
                .build();
    }

    public static CallerType toCoreCallerType(com.netflix.titus.grpc.protogen.CallMetadata.CallerType grpcCallerType) {
        switch (grpcCallerType) {
            case Application:
                return CallerType.Application;
            case User:
                return CallerType.User;
            case Unknown:
            case UNRECOGNIZED:
            default:
                return CallerType.Unknown;
        }
    }

    private static Caller toCoreCaller(com.netflix.titus.grpc.protogen.CallMetadata.Caller grpcCaller) {
        return Caller.newBuilder()
                .withId(grpcCaller.getId())
                .withCallerType(toCoreCallerType(grpcCaller.getType()))
                .withContext(grpcCaller.getContextMap())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.CallMetadata toGrpcCallMetadata(CallMetadata callMetadata) {
        return com.netflix.titus.grpc.protogen.CallMetadata.newBuilder()
                .setCallerId(callMetadata.getCallerId())
                .addAllCallers(callMetadata.getCallers().stream().map(CommonGrpcModelConverters2::toGrpcCaller).collect(Collectors.toList()))
                .setCallReason(callMetadata.getCallReason())
                .addAllCallPath(callMetadata.getCallPath())
                .setDebug(callMetadata.isDebug())
                .build();
    }

    private static com.netflix.titus.grpc.protogen.CallMetadata.CallerType toGrpcCallerType(CallerType callerType) {
        if (callerType == null) {
            return com.netflix.titus.grpc.protogen.CallMetadata.CallerType.Unknown;
        }
        switch (callerType) {
            case Application:
                return com.netflix.titus.grpc.protogen.CallMetadata.CallerType.Application;
            case User:
                return com.netflix.titus.grpc.protogen.CallMetadata.CallerType.User;
            case Unknown:
            default:
                return com.netflix.titus.grpc.protogen.CallMetadata.CallerType.Unknown;
        }
    }

    private static com.netflix.titus.grpc.protogen.CallMetadata.Caller toGrpcCaller(Caller coreCaller) {
        return com.netflix.titus.grpc.protogen.CallMetadata.Caller.newBuilder()
                .setId(coreCaller.getId())
                .setType(toGrpcCallerType(coreCaller.getCallerType()))
                .putAllContext(coreCaller.getContext())
                .build();
    }
}
