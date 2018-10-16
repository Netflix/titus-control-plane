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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import io.grpc.stub.StreamObserver;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.api.jobmanager.JobAttributes.JOB_ATTRIBUTES_CELL;

class CellWithCachedJobsService extends JobManagementServiceGrpc.JobManagementServiceImplBase {
    private final String cellName;
    private final Map<JobId, JobDescriptor> jobDescriptorMap = new HashMap<>();

    CellWithCachedJobsService(String name) {
        this.cellName = name;
    }

    Optional<JobDescriptor> getCachedJob(String jobId) {
        return Optional.ofNullable(jobDescriptorMap.get(JobId.newBuilder().setId(jobId).build()));
    }

    @Override
    public void createJob(JobDescriptor request, StreamObserver<JobId> responseObserver) {
        JobId jobId = JobId.newBuilder().setId(UUID.randomUUID().toString()).build();
        jobDescriptorMap.put(
                jobId,
                JobDescriptor.newBuilder(request)
                        .putAttributes(JOB_ATTRIBUTES_CELL, cellName)
                        .build());
        Subscription subscription = Observable.just(jobId)
                .subscribe(
                        responseObserver::onNext,
                        responseObserver::onError,
                        responseObserver::onCompleted
                );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void findJob(JobId request, StreamObserver<Job> responseObserver) {
        final Subscription subscription = Observable.just(Job.newBuilder().setJobDescriptor(jobDescriptorMap.get(request)).build())
                .subscribe(
                        responseObserver::onNext,
                        responseObserver::onError,
                        responseObserver::onCompleted
                );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }
}

