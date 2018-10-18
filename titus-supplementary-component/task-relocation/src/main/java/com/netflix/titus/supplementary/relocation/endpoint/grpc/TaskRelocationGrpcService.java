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

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.RelocationEvent;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;
import io.grpc.stub.StreamObserver;

@Singleton
public class TaskRelocationGrpcService extends TaskRelocationServiceGrpc.TaskRelocationServiceImplBase {

    private final RelocationWorkflowExecutor relocationWorkflowExecutor;

    @Inject
    public TaskRelocationGrpcService(RelocationWorkflowExecutor relocationWorkflowExecutor) {
        this.relocationWorkflowExecutor = relocationWorkflowExecutor;
    }

    @Override
    public void getCurrentTaskRelocationPlans(TaskRelocationQuery request, StreamObserver<TaskRelocationPlans> responseObserver) {
        List<TaskRelocationPlan> corePlans = new ArrayList<>(relocationWorkflowExecutor.getPlannedRelocations().values());
        TaskRelocationPlans grpcPlans = RelocationGrpcModelConverters.toGrpcTaskRelocationPlans(corePlans);

        responseObserver.onNext(grpcPlans);
        responseObserver.onCompleted();
    }

    /**
     * TODO Implement filtering.
     */
    @Override
    public void getTaskRelocationResult(TaskRelocationQuery request, StreamObserver<TaskRelocationExecutions> responseObserver) {
        List<TaskRelocationStatus> coreResults = new ArrayList<>(relocationWorkflowExecutor.getLastEvictionResults().values());
        TaskRelocationExecutions grpcResults = RelocationGrpcModelConverters.toGrpcTaskRelocationExecutions(coreResults);

        responseObserver.onNext(grpcResults);
        responseObserver.onCompleted();
    }

    /**
     * TODO Implement
     */
    @Override
    public void observeRelocationEvents(TaskRelocationQuery request, StreamObserver<RelocationEvent> responseObserver) {
        responseObserver.onError(new RuntimeException("not implemented yet"));
    }
}
