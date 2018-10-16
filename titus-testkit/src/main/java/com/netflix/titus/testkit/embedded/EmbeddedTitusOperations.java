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

package com.netflix.titus.testkit.embedded;

import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import rx.Observable;

public interface EmbeddedTitusOperations {
    SimulatedCloud getSimulatedCloud();

    HealthGrpc.HealthStub getHealthClient();

    JobManagementServiceGrpc.JobManagementServiceStub getV3GrpcClient();

    JobManagementServiceGrpc.JobManagementServiceBlockingStub getV3BlockingGrpcClient();

    AgentManagementServiceGrpc.AgentManagementServiceStub getV3GrpcAgentClient();

    AgentManagementServiceGrpc.AgentManagementServiceBlockingStub getV3BlockingGrpcAgentClient();

    AutoScalingServiceGrpc.AutoScalingServiceStub getAutoScaleGrpcClient();

    LoadBalancerServiceGrpc.LoadBalancerServiceStub getLoadBalancerGrpcClient();

    Observable<TaskExecutorHolder> observeLaunchedTasks();

    Observable<TaskExecutorHolder> awaitTaskExecutorHolderOf(String taskId);
}
