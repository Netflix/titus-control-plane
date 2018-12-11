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
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.grpc.protogen.RelocationEvent;
import com.netflix.titus.grpc.protogen.RelocationTaskId;
import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.grpc.protogen.TaskRelocationServiceGrpc;
import com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowExecutor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.attachCancellingCallback;
import static com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil.safeOnError;
import static com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters.toGrpcTaskRelocationExecutions;
import static com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationPlanPredicate.buildProtobufQueryResult;

@Singleton
public class TaskRelocationGrpcService extends TaskRelocationServiceGrpc.TaskRelocationServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(TaskRelocationGrpcService.class);

    private final ReadOnlyJobOperations jobOperations;
    private final RelocationWorkflowExecutor relocationWorkflowExecutor;
    private final TaskRelocationResultStore archiveStore;

    @Inject
    public TaskRelocationGrpcService(ReadOnlyJobOperations jobOperations,
                                     RelocationWorkflowExecutor relocationWorkflowExecutor,
                                     TaskRelocationResultStore archiveStore) {
        this.jobOperations = jobOperations;
        this.relocationWorkflowExecutor = relocationWorkflowExecutor;
        this.archiveStore = archiveStore;
    }

    /**
     * TODO Pagination once the core pagination model with cursor is available.
     */
    @Override
    public void getCurrentTaskRelocationPlans(TaskRelocationQuery request, StreamObserver<TaskRelocationPlans> responseObserver) {
        responseObserver.onNext(buildProtobufQueryResult(jobOperations, relocationWorkflowExecutor, request));
        responseObserver.onCompleted();
    }

    /**
     * TODO Implement filtering.
     */
    @Override
    public void getLatestTaskRelocationResults(TaskRelocationQuery request, StreamObserver<TaskRelocationExecutions> responseObserver) {
        List<TaskRelocationStatus> coreResults = new ArrayList<>(relocationWorkflowExecutor.getLastEvictionResults().values());
        TaskRelocationExecutions grpcResults = toGrpcTaskRelocationExecutions(coreResults);

        responseObserver.onNext(grpcResults);
        responseObserver.onCompleted();
    }

    @Override
    public void getTaskRelocationResult(RelocationTaskId request, StreamObserver<TaskRelocationExecution> responseObserver) {
        String taskId = request.getId();

        TaskRelocationStatus latest = relocationWorkflowExecutor.getLastEvictionResults().get(taskId);

        Disposable disposable = archiveStore.getTaskRelocationStatusList(taskId).subscribe(
                archived -> {
                    if (latest == null && archived.isEmpty()) {
                        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
                        return;
                    }

                    List<TaskRelocationStatus> combined;
                    if (latest == null) {
                        combined = archived;
                    } else if (archived.isEmpty()) {
                        combined = Collections.singletonList(latest);
                    } else {
                        if (CollectionsExt.last(archived).equals(latest)) {
                            combined = archived;
                        } else {
                            combined = CollectionsExt.copyAndAdd(archived, latest);
                        }
                    }

                    responseObserver.onNext(RelocationGrpcModelConverters.toGrpcTaskRelocationExecution(combined));
                },
                e -> safeOnError(logger, e, responseObserver),
                responseObserver::onCompleted
        );
        attachCancellingCallback(responseObserver, disposable);
    }

    /**
     * TODO Implement
     */
    @Override
    public void observeRelocationEvents(TaskRelocationQuery request, StreamObserver<RelocationEvent> responseObserver) {
        responseObserver.onError(new RuntimeException("not implemented yet"));
    }
}
