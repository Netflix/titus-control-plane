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

package com.netflix.titus.common.util.loadshedding.grpc;

import java.util.function.Supplier;

import com.netflix.titus.common.util.loadshedding.AdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerResponse;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcAdmissionControllerServerInterceptor implements ServerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(GrpcAdmissionControllerServerInterceptor.class);

    private final AdmissionController admissionController;
    private final Supplier<String> callerIdResolver;

    public GrpcAdmissionControllerServerInterceptor(AdmissionController admissionController,
                                                    Supplier<String> callerIdResolver) {
        this.admissionController = admissionController;
        this.callerIdResolver = callerIdResolver;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        AdmissionControllerResponse result;
        try {
            AdmissionControllerRequest request = AdmissionControllerRequest.newBuilder()
                    .withCallerId(callerIdResolver.get())
                    .withEndpointName(call.getMethodDescriptor().getFullMethodName())
                    .build();
            result = admissionController.apply(request);
        } catch (Exception e) {
            logger.warn("Admission controller error: {}", e.getMessage());
            logger.debug("Stack trace", e);

            return next.startCall(call, headers);
        }

        if (result.isAllowed()) {
            return next.startCall(call, headers);
        }

        call.close(Status.RESOURCE_EXHAUSTED.withDescription(result.getReasonMessage()), new Metadata());
        return new ServerCall.Listener<ReqT>() {
        };
    }
}
