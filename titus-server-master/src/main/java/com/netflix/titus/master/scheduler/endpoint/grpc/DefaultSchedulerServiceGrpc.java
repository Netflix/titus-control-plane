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

package com.netflix.titus.master.scheduler.endpoint.grpc;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Injector;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failure;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failures;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent.Success;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import com.netflix.titus.master.kubernetes.client.DirectKubeApiServerIntegrator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;

@Singleton
public class DefaultSchedulerServiceGrpc extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private final V3JobOperations v3JobOperations;
    private final Injector injector;

    @Inject
    public DefaultSchedulerServiceGrpc(V3JobOperations v3JobOperations, Injector injector) {
        this.v3JobOperations = v3JobOperations;
        this.injector = injector;
    }

    @Override
    public void getSchedulingResult(SchedulingResultRequest request, StreamObserver<com.netflix.titus.grpc.protogen.SchedulingResultEvent> responseObserver) {
        String taskId = request.getTaskId();
        Pair<Job<?>, Task> jobAndTask = v3JobOperations.findTaskById(taskId).orElse(null);
        if (jobAndTask == null) {
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Task not found: " + taskId)));
            return;
        }

        DirectKubeApiServerIntegrator directIntegrator = injector.getInstance(DirectKubeApiServerIntegrator.class);
        V1Pod pod = directIntegrator.findPod(taskId).orElse(null);
        if (pod != null) {
            responseObserver.onNext(toGrpcSchedulingResultEvent(pod));
            responseObserver.onCompleted();
            return;
        }

        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("No scheduling result available for task: " + taskId)));
    }

    private com.netflix.titus.grpc.protogen.SchedulingResultEvent toGrpcSchedulingResultEvent(V1Pod pod) {
        if ("TASK_RUNNING".equalsIgnoreCase(pod.getStatus().getPhase())) {
            return com.netflix.titus.grpc.protogen.SchedulingResultEvent.newBuilder()
                    .setSuccess(Success.newBuilder().setMessage("Running now"))
                    .build();
        }

        Failures.Builder failuresBuilder = Failures.newBuilder();

        if (CollectionsExt.isNullOrEmpty(pod.getStatus().getConditions())) {
            failuresBuilder.addFailures(Failure.newBuilder()
                    .setReason("Task not scheduled yet, but no placement issues found")
                    .setFailureCount(1)
            );
        } else {
            for (V1PodCondition v1PodCondition : pod.getStatus().getConditions()) {
                failuresBuilder.addFailures(Failure.newBuilder()
                        .setReason(String.format("Pod scheduling failure: reason=%s, message=%s",
                                v1PodCondition.getReason(), v1PodCondition.getMessage()
                        ))
                        .setFailureCount(1)
                );
            }
        }

        return com.netflix.titus.grpc.protogen.SchedulingResultEvent.newBuilder().setFailures(failuresBuilder).build();
    }
}
