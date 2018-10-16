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

package com.netflix.titus.supplementary.relocation.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.RelocationEvent;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import io.grpc.stub.StreamObserver;

import static com.netflix.titus.supplementary.relocation.endpoint.StubRequestReplies.STUB_RELOCATION_PLAN;
import static com.netflix.titus.supplementary.relocation.endpoint.StubRequestReplies.STUB_RELOCATION_PLANS;
import static com.netflix.titus.supplementary.relocation.endpoint.StubRequestReplies.STUB_RELOCATION_EXECUTIONS;

@Singleton
public class TaskRelocationGrpcService extends TaskRelocationServiceGrpc.TaskRelocationServiceImplBase {

    @Inject
    public TaskRelocationGrpcService() {
    }

    /**
     * TODO Stub implementation
     */
    @Override
    public void getCurrentTaskRelocationPlans(TaskRelocationQuery request, StreamObserver<TaskRelocationPlans> responseObserver) {
        responseObserver.onNext(STUB_RELOCATION_PLANS);
        responseObserver.onCompleted();
    }

    /**
     * TODO Stub implementation
     */
    @Override
    public void getTaskRelocationResult(TaskRelocationQuery request, StreamObserver<TaskRelocationExecutions> responseObserver) {
        responseObserver.onNext(STUB_RELOCATION_EXECUTIONS);
        responseObserver.onCompleted();
    }

    /**
     * TODO Stub implementation
     */
    @Override
    public void observeRelocationEvents(TaskRelocationQuery request, StreamObserver<RelocationEvent> responseObserver) {
        responseObserver.onNext(RelocationEvent.newBuilder()
                .setTaskRelocationPlanEvent(RelocationEvent.TaskRelocationPlanEvent.newBuilder().setPlan(STUB_RELOCATION_PLAN))
                .build()
        );
    }
}
