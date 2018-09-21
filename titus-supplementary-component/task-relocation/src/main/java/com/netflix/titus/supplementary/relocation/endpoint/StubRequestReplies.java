package com.netflix.titus.supplementary.relocation.endpoint;

import com.netflix.titus.grpc.protogen.TaskRelocationExecution;
import com.netflix.titus.grpc.protogen.TaskRelocationExecutions;
import com.netflix.titus.grpc.protogen.TaskRelocationPlan;
import com.netflix.titus.grpc.protogen.TaskRelocationPlans;
import com.netflix.titus.grpc.protogen.TaskRelocationStatus;

// TODO remove once proper implementation is done
public class StubRequestReplies {

    public static final TaskRelocationPlan STUB_RELOCATION_PLAN = TaskRelocationPlan.newBuilder()
            .setTaskId("fakeTaskId")
            .setReasonCode("stubReasonCode")
            .setReasonMessage("stubMessage")
            .setRelocationTime(System.currentTimeMillis())
            .build();

    public static final TaskRelocationExecution STUB_RELOCATION_EXECUTION = TaskRelocationExecution.newBuilder()
            .setTaskRelocationPlan(STUB_RELOCATION_PLAN)
            .addRelocationAttempts(TaskRelocationStatus.newBuilder()
                    .setReasonCode("stubReasonCode")
            )
            .build();

    public static final TaskRelocationPlans STUB_RELOCATION_PLANS = TaskRelocationPlans.newBuilder().addPlans(STUB_RELOCATION_PLAN).build();

    public static final TaskRelocationExecutions STUB_RELOCATION_EXECUTIONS = TaskRelocationExecutions.newBuilder().addResults(STUB_RELOCATION_EXECUTION).build();
}
