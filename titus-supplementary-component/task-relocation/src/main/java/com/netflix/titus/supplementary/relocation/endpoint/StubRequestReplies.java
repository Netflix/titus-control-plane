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
