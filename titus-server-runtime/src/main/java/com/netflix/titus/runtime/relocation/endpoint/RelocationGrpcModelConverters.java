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

package com.netflix.titus.runtime.relocation.endpoint;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus.TaskRelocationState;
import com.netflix.titus.api.relocation.model.event.TaskRelocationEvent;
import com.netflix.titus.api.relocation.model.event.TaskRelocationPlanRemovedEvent;
import com.netflix.titus.api.relocation.model.event.TaskRelocationPlanUpdateEvent;
import com.netflix.titus.grpc.protogen.RelocationEvent;
import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;

import static com.netflix.titus.common.util.CollectionsExt.last;

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
                .setDecisionTime(plan.getDecisionTime())
                .setRelocationTime(plan.getRelocationTime())
                .build();
    }

    public static TaskRelocationPlan toCoreTaskRelocationPlan(com.netflix.titus.grpc.protogen.TaskRelocationPlan grpcPlan) {
        return TaskRelocationPlan.newBuilder()
                .withTaskId(grpcPlan.getTaskId())
                .withReason(TaskRelocationPlan.TaskRelocationReason.valueOf(grpcPlan.getReasonCode()))
                .withReasonMessage(grpcPlan.getReasonMessage())
                .withDecisionTime(grpcPlan.getDecisionTime())
                .withRelocationTime(grpcPlan.getRelocationTime())
                .build();
    }

    public static TaskRelocationExecution toGrpcTaskRelocationExecution(List<TaskRelocationStatus> attempts) {
        Preconditions.checkArgument(!attempts.isEmpty(), "Empty list of TaskRelocationStatus objects");

        return TaskRelocationExecution.newBuilder()
                .setTaskRelocationPlan(toGrpcTaskRelocationPlan(last(attempts).getTaskRelocationPlan()))
                .addAllRelocationAttempts(attempts.stream().map(RelocationGrpcModelConverters::toGrpcTaskRelocationStatus).collect(Collectors.toList()))
                .build();
    }

    public static TaskRelocationExecution toGrpcTaskRelocationExecution(TaskRelocationStatus coreResult) {
        return TaskRelocationExecution.newBuilder()
                .addRelocationAttempts(toGrpcTaskRelocationStatus(coreResult))
                .setTaskRelocationPlan(toGrpcTaskRelocationPlan(coreResult.getTaskRelocationPlan()))
                .build();
    }

    public static TaskRelocationExecutions toGrpcTaskRelocationExecutions(List<TaskRelocationStatus> coreResults) {
        return TaskRelocationExecutions.newBuilder()
                .addAllResults(coreResults.stream().map(RelocationGrpcModelConverters::toGrpcTaskRelocationExecution).collect(Collectors.toList()))
                .build();
    }

    public static com.netflix.titus.grpc.protogen.TaskRelocationStatus toGrpcTaskRelocationStatus(TaskRelocationStatus coreResult) {
        return com.netflix.titus.grpc.protogen.TaskRelocationStatus.newBuilder()
                .setState(toGrpcRelocationState(coreResult.getState()))
                .setStatusCode(coreResult.getStatusCode())
                .setStatusMessage(coreResult.getStatusMessage())
                .build();
    }

    public static com.netflix.titus.grpc.protogen.TaskRelocationStatus.TaskRelocationState toGrpcRelocationState(TaskRelocationState coreState) {
        switch (coreState) {
            case Success:
                return com.netflix.titus.grpc.protogen.TaskRelocationStatus.TaskRelocationState.Success;
            case Failure:
                return com.netflix.titus.grpc.protogen.TaskRelocationStatus.TaskRelocationState.Failure;
        }
        throw new IllegalStateException("Unrecognized state: " + coreState);
    }

    public static Optional<RelocationEvent> toGrpcRelocationEvent(TaskRelocationEvent coreEvent) {
        RelocationEvent grpcEvent = null;
        if (coreEvent.equals(TaskRelocationEvent.newSnapshotEndEvent())) {
            grpcEvent = RelocationEvent.newBuilder()
                    .setSnapshotEnd(RelocationEvent.SnapshotEnd.getDefaultInstance())
                    .build();
        } else if (coreEvent instanceof TaskRelocationPlanUpdateEvent) {
            TaskRelocationPlanUpdateEvent updateEvent = (TaskRelocationPlanUpdateEvent) coreEvent;
            grpcEvent = RelocationEvent.newBuilder()
                    .setTaskRelocationPlanUpdateEvent(RelocationEvent.TaskRelocationPlanUpdateEvent.newBuilder()
                            .setPlan(toGrpcTaskRelocationPlan(updateEvent.getPlan()))
                    )
                    .build();
        } else if (coreEvent instanceof TaskRelocationPlanRemovedEvent) {
            TaskRelocationPlanRemovedEvent removedEvent = (TaskRelocationPlanRemovedEvent) coreEvent;
            grpcEvent = RelocationEvent.newBuilder()
                    .setTaskRelocationPlanRemoveEvent(RelocationEvent.TaskRelocationPlanRemoveEvent.newBuilder()
                            .setTaskId(removedEvent.getTaskId())
                    )
                    .build();
        }
        return Optional.ofNullable(grpcEvent);
    }

    public static Optional<TaskRelocationEvent> toCoreRelocationEvent(RelocationEvent grpcEvent) {
        switch (grpcEvent.getEventCase()) {
            case SNAPSHOTEND:
                return Optional.of(TaskRelocationEvent.newSnapshotEndEvent());
            case TASKRELOCATIONPLANUPDATEEVENT:
                return Optional.of(TaskRelocationEvent.taskRelocationPlanUpdated(toCoreTaskRelocationPlan(grpcEvent.getTaskRelocationPlanUpdateEvent().getPlan())));
            case TASKRELOCATIONPLANREMOVEEVENT:
                return Optional.of(TaskRelocationEvent.taskRelocationPlanRemoved(grpcEvent.getTaskRelocationPlanRemoveEvent().getTaskId()));
            case TASKRELOCATIONRESULTEVENT:
            case EVENT_NOT_SET:
            default:
                return Optional.empty();
        }
    }
}
