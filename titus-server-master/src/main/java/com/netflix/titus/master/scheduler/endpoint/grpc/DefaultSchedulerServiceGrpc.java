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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.Empty;
import com.netflix.fenzo.ConstraintFailure;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.titus.api.scheduler.service.SchedulerService;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.grpc.protogen.SchedulingResultRequest;
import com.netflix.titus.grpc.protogen.SystemSelector;
import com.netflix.titus.grpc.protogen.SystemSelectorId;
import com.netflix.titus.grpc.protogen.SystemSelectorUpdate;
import com.netflix.titus.grpc.protogen.SystemSelectors;
import com.netflix.titus.master.scheduler.SchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingResultEvent.FailedSchedulingResultEvent;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcSchedulerModelConverters;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

@Singleton
public class DefaultSchedulerServiceGrpc extends SchedulerServiceGrpc.SchedulerServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulerServiceGrpc.class);

    private static final int AGENT_SAMPLE_SIZE = 5;

    private final SchedulerService schedulerService;
    private final SchedulingService schedulingService;
    private final CallMetadataResolver callMetadataResolver;

    @Inject
    public DefaultSchedulerServiceGrpc(SchedulerService schedulerService,
                                       SchedulingService schedulingService,
                                       CallMetadataResolver callMetadataResolver) {
        this.schedulerService = schedulerService;
        this.schedulingService = schedulingService;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public void getSystemSelectors(Empty request, StreamObserver<SystemSelectors> responseObserver) {
        execute(responseObserver, user -> {
            List<SystemSelector> all = schedulerService.getSystemSelectors().stream()
                    .map(GrpcSchedulerModelConverters::toGrpcSystemSelector)
                    .collect(Collectors.toList());
            responseObserver.onNext(SystemSelectors.newBuilder().addAllSystemSelectors(all).build());
        });
    }

    @Override
    public void getSystemSelector(SystemSelectorId request, StreamObserver<SystemSelector> responseObserver) {
        execute(responseObserver, user -> {
            com.netflix.titus.api.scheduler.model.SystemSelector systemSelector = schedulerService.getSystemSelector(request.getId());
            SystemSelector grpcSystemSelector = GrpcSchedulerModelConverters.toGrpcSystemSelector(systemSelector);
            responseObserver.onNext(grpcSystemSelector);
        });
    }

    @Override
    public void createSystemSelector(SystemSelector request, StreamObserver<Empty> responseObserver) {
        schedulerService.createSystemSelector(
                GrpcSchedulerModelConverters.toCoreSystemSelector(request)
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void updateSystemSelector(SystemSelectorUpdate request, StreamObserver<Empty> responseObserver) {
        schedulerService.updateSystemSelector(
                request.getId(),
                GrpcSchedulerModelConverters.toCoreSystemSelector(request.getSystemSelector())
        ).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    @Override
    public void deleteSystemSelector(SystemSelectorId request, StreamObserver<Empty> responseObserver) {
        schedulerService.deleteSystemSelector(request.getId()).subscribe(
                () -> {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                },
                responseObserver::onError
        );
    }

    /**
     * TODO Scheduling component does not provide a data model for the scheduling results. We are forced to use Fenzo model here.
     */
    @Override
    public void observeSchedulingResults(SchedulingResultRequest request, StreamObserver<com.netflix.titus.grpc.protogen.SchedulingResultEvent> responseObserver) {
        Subscription subscription = schedulingService.observeSchedulingResults(request.getTaskId()).subscribe(
                next -> responseObserver.onNext(toGrpcSchedulingResultEvent(next)),
                e -> {
                    Status status;
                    if (e instanceof TimeoutException) {
                        status = Status.DEADLINE_EXCEEDED;
                    } else if (e instanceof IllegalArgumentException) {
                        status = Status.NOT_FOUND;
                    } else {
                        status = Status.INTERNAL;
                    }
                    responseObserver.onError(new StatusRuntimeException(status.withCause(e)));
                },
                responseObserver::onCompleted
        );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void getSchedulingResult(SchedulingResultRequest request, StreamObserver<com.netflix.titus.grpc.protogen.SchedulingResultEvent> responseObserver) {
        String taskId = request.getTaskId();
        Optional<SchedulingResultEvent> result = schedulingService.findLastSchedulingResult(taskId);
        if (result.isPresent()) {
            responseObserver.onNext(toGrpcSchedulingResultEvent(result.get()));
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Task not found: " + taskId)));
        }
    }

    private com.netflix.titus.grpc.protogen.SchedulingResultEvent toGrpcSchedulingResultEvent(SchedulingResultEvent event) {
        if (event instanceof SchedulingResultEvent.SuccessfulSchedulingResultEvent) {
            return com.netflix.titus.grpc.protogen.SchedulingResultEvent.newBuilder()
                    .setSuccess(com.netflix.titus.grpc.protogen.SchedulingResultEvent.Success.newBuilder()
                            .setMessage("Task in state: " + event.getTask().getStatus().getState())
                    )
                    .build();
        }

        FailedSchedulingResultEvent failedEvent = (FailedSchedulingResultEvent) event;
        if (failedEvent.getAssignmentResults().isEmpty()) {
            return com.netflix.titus.grpc.protogen.SchedulingResultEvent.newBuilder()
                    .setFailures(com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failures.newBuilder()
                            .addFailures(com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failure.newBuilder()
                                    .setReason("No agents available")
                                    .setFailureCount(1)
                            )
                    )
                    .build();
        }

        Map<String, List<String>> agentSamples = new HashMap<>();
        Map<String, Integer> failureCounts = new HashMap<>();
        for (TaskAssignmentResult schedulingResult : failedEvent.getAssignmentResults()) {
            String failureId;

            ConstraintFailure constraintFailure = schedulingResult.getConstraintFailure();
            if (constraintFailure != null) {
                failureId = constraintFailure.getReason();
            } else {
                if (CollectionsExt.isNullOrEmpty(schedulingResult.getFailures())) {
                    failureId = "UNKNOWN";
                } else {
                    failureId = schedulingResult.getFailures().get(0).getMessage();
                }
            }

            List<String> agents = agentSamples.computeIfAbsent(failureId, name -> new ArrayList<>());
            if (agents.size() < AGENT_SAMPLE_SIZE && StringExt.isNotEmpty(schedulingResult.getHostname())) {
                agents.add(schedulingResult.getHostname());
            }
            failureCounts.put(failureId, failureCounts.getOrDefault(failureId, 0) + 1);
        }

        com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failures.Builder builder = com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failures.newBuilder();
        for (String failureId : agentSamples.keySet()) {
            com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failure failure = com.netflix.titus.grpc.protogen.SchedulingResultEvent.Failure.newBuilder()
                    .setReason(failureId)
                    .addAllAgentIdSamples(agentSamples.get(failureId))
                    .setFailureCount(failureCounts.get(failureId))
                    .build();
            builder.addFailures(failure);
        }
        return com.netflix.titus.grpc.protogen.SchedulingResultEvent.newBuilder().setFailures(builder.build()).build();
    }

    private void execute(StreamObserver<?> responseObserver, Consumer<CallMetadata> action) {
        Optional<CallMetadata> callMetadata = callMetadataResolver.resolve();
        if (!callMetadata.isPresent()) {
            responseObserver.onError(TitusServiceException.noCallerId());
            return;
        }
        try {
            action.accept(callMetadata.get());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
