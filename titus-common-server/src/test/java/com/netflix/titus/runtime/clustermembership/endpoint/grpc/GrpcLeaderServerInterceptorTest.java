/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime.clustermembership.endpoint.grpc;

import com.netflix.titus.grpc.protogen.ClusterMembershipServiceGrpc;
import com.netflix.titus.testing.SampleServiceGrpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServiceDescriptor;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcLeaderServerInterceptorTest {

    private static final Metadata METADATA = new Metadata();

    private final ServerCallHandler serverCallHandler = mock(ServerCallHandler.class);

    @Test
    public void testClusterMembershipMethodIsAllowed() {
        testIsCallAllowed(
                GrpcLeaderServerInterceptor.clusterMembershipAllowed(() -> false),
                newServerCall(getMethod(ClusterMembershipServiceGrpc.getServiceDescriptor(), "GetMembers"))
        );
    }

    @Test
    public void testBusinessMethodIsNotAllowed() {
        testIsNotCallAllowed(
                GrpcLeaderServerInterceptor.clusterMembershipAllowed(() -> false),
                newServerCall(getMethod(SampleServiceGrpc.getServiceDescriptor(), "GetOneValue"))
        );
    }

    @Test
    public void testActiveLeaderIsAlwaysAllowed() {
        testIsCallAllowed(
                GrpcLeaderServerInterceptor.clusterMembershipAllowed(() -> true),
                newServerCall(getMethod(ClusterMembershipServiceGrpc.getServiceDescriptor(), "GetMembers"))
        );

        testIsCallAllowed(
                GrpcLeaderServerInterceptor.clusterMembershipAllowed(() -> true),
                newServerCall(getMethod(SampleServiceGrpc.getServiceDescriptor(), "GetOneValue"))
        );
    }

    private ServerCall newServerCall(MethodDescriptor<Object, Object> methodDescriptor) {
        ServerCall<Object, Object> serverCall = mock(ServerCall.class);
        when(serverCall.getMethodDescriptor()).thenReturn(methodDescriptor);
        return serverCall;
    }

    private void testIsCallAllowed(GrpcLeaderServerInterceptor interceptor, ServerCall serverCall) {
        interceptor.interceptCall(serverCall, METADATA, serverCallHandler);
        verify(serverCallHandler, times(1)).startCall(serverCall, METADATA);
    }

    private void testIsNotCallAllowed(GrpcLeaderServerInterceptor interceptor, ServerCall serverCall) {
        interceptor.interceptCall(serverCall, METADATA, serverCallHandler);
        verify(serverCallHandler, times(0)).startCall(serverCall, METADATA);
        verify(serverCall, times(1)).close(any(), any());
    }

    private MethodDescriptor getMethod(ServiceDescriptor serviceDescriptor, String methodName) {
        MethodDescriptor<?, ?> methodDescriptor = serviceDescriptor.getMethods().stream()
                .filter(m -> m.getFullMethodName().contains(methodName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Method not found: " + methodName));

        // Rebuild to get a different version
        return methodDescriptor.toBuilder().build();
    }
}