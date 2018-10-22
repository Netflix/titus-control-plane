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

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;

public class RelocationGrpcModelConverters {

    public static TaskRelocationPlans toGrpcTaskRelocationPlans(List<TaskRelocationPlan> corePlans) {
        return TaskRelocationPlans.newBuilder()
                .addAllPlans(corePlans.stream().map(RelocationGrpcModelConverters::toGrpcTaskRelocationPlan).collect(Collectors.toList()))
                .build();
    }

    public static com.netflix.titus.grpc.protogen.TaskRelocationPlan toGrpcTaskRelocationPlan(TaskRelocationPlan plan) {
        return com.netflix.titus.grpc.protogen.TaskRelocationPlan.newBuilder()
                .setTaskId(plan.getTaskId())
                .setReasonCode(plan.getReason().name())
                .setReasonMessage(plan.getReasonMessage())
                .setRelocationTime(plan.getRelocationTime())
                .build();
    }

    public static TaskRelocationExecutions toGrpcTaskRelocationExecutions(List<TaskRelocationStatus> coreResults) {
        return TaskRelocationExecutions.newBuilder()
                .addAllResults(coreResults.stream().map(RelocationGrpcModelConverters::toTaskRelocationExecution).collect(Collectors.toList()))
                .build();
    }

    private static TaskRelocationExecution toTaskRelocationExecution(TaskRelocationStatus coreResult) {
        return TaskRelocationExecution.newBuilder()
                .addRelocationAttempts(toGrpcTaskRelocationStatus(coreResult))
                .setTaskRelocationPlan(toGrpcTaskRelocationPlan(coreResult.getTaskRelocationPlan()))
                .build();
    }

    public static com.netflix.titus.grpc.protogen.TaskRelocationStatus toGrpcTaskRelocationStatus(TaskRelocationStatus coreResult) {
        return com.netflix.titus.grpc.protogen.TaskRelocationStatus.newBuilder()
                .setReasonCode(coreResult.getReasonCode())
                .build();
    }
}
