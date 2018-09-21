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
