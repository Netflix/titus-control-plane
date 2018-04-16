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

package com.netflix.titus.federation.service;

import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import io.grpc.stub.StreamObserver;
import rx.Observable;
import rx.Subscription;

import static io.grpc.Status.NOT_FOUND;

class CellWithJobStream extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private final String jobId;
    private final Observable<JobChangeNotification> updates;

    CellWithJobStream(String jobId, Observable<JobChangeNotification> updates) {
        this.jobId = jobId;
        this.updates = updates;
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        if (!jobId.equals(request.getId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        responseObserver.onNext(Job.newBuilder().setId(jobId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void observeJob(JobId request, StreamObserver<JobChangeNotification> responseObserver) {
        if (!jobId.equals(request.getId())) {
            responseObserver.onError(NOT_FOUND.asRuntimeException());
            return;
        }
        final Subscription subscription = updates.subscribe(
                responseObserver::onNext,
                responseObserver::onError,
                responseObserver::onCompleted
        );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }
}
