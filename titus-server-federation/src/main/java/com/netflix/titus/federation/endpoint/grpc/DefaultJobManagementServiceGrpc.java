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

package com.netflix.titus.federation.endpoint.grpc;

import javax.inject.Inject;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.JobQueryResult;
import io.grpc.stub.StreamObserver;
import com.netflix.titus.federation.service.JobManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.endpoint.v3.grpc.TitusPaginationUtils.checkPageIsValid;

public class DefaultJobManagementServiceGrpc extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJobManagementServiceGrpc.class);

    private final JobManagementService jobManagementService;

    @Inject
    public DefaultJobManagementServiceGrpc(JobManagementService jobManagementService) {
        this.jobManagementService = jobManagementService;
    }

    @Override
    public void findJobs(JobQuery request, StreamObserver<JobQueryResult> responseObserver) {
        if (!checkPageIsValid(request.getPage(), responseObserver)) {
            return;
        }

        final Subscription subscription = jobManagementService.findJobs(request).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void observeJobs(Empty request, StreamObserver<JobChangeNotification> responseObserver) {
        final Subscription subscription = jobManagementService.observeJobs().subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }
}
