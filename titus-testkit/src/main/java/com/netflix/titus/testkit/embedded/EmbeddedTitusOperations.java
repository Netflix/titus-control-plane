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
