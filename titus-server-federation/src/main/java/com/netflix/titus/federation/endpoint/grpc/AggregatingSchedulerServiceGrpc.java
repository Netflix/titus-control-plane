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

package com.netflix.titus.federation.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.federation.service.AggregatingSchedulerService;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;

@Singleton
public class AggregatingSchedulerServiceGrpc extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(AggregatingSchedulerServiceGrpc.class);

    private final AggregatingSchedulerService schedulerService;

    @Inject
    public AggregatingSchedulerServiceGrpc(AggregatingSchedulerService schedulerService) {
        this.schedulerService = schedulerService;
    }

    @Override
    public void getSchedulingResult(SchedulingResultRequest request, StreamObserver<SchedulingResultEvent> responseObserver) {
        Disposable subscription = schedulerService.findLastSchedulingResult(request.getTaskId()).subscribe(
                responseObserver::onNext,
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, subscription);
    }
}
