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
package com.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.grpc.AnonymousSessionContext;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Rule;

import static com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceImplBase;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AggregatingAutoScalingTestBase {
    static final String POLICY_1 = "policy1";
    static final String POLICY_2 = "policy2";
    static final String JOB_1 = "job1";
    static final String JOB_2 = "job2";

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();

    AggregatingAutoScalingService service;

    @Before
    public void setUp() {
        CellConnector connector = mock(CellConnector.class);
        Map<Cell, ManagedChannel> cellMap = new HashMap<>();
        cellMap.put(new Cell("one", "1"), cellOne.getChannel());
        cellMap.put(new Cell("two", "2"), cellTwo.getChannel());
        when(connector.getChannels()).thenReturn(cellMap);
        when(connector.getChannelForCell(any())).then(invocation ->
                Optional.ofNullable(cellMap.get(invocation.getArgument(0)))
        );

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(1000L);
        service = new AggregatingAutoScalingService(connector, new AnonymousSessionContext(), grpcConfiguration);
    }

    static class CellWithJobs extends JobManagementServiceImplBase {
        private List<String> jobIds;

        CellWithJobs(List<String> jobIds) {
            this.jobIds = jobIds;
        }

        @Override
        public void findJob(JobId request, StreamObserver<Job> responseObserver) {
            if (jobIds.contains(request.getId())) {
                responseObserver.onNext(Job.newBuilder().setId(request.getId()).build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
        }
    }

    static class CellWithPolicies extends AutoScalingServiceImplBase {
        private final Map<String, ScalingPolicyResult> policyMap;

        CellWithPolicies(List<ScalingPolicyResult> policyResults) {
            policyMap = policyResults.stream().collect(Collectors.toMap(p -> p.getId().getId(), p -> p));
        }

        @Override
        public void getAllScalingPolicies(Empty request, StreamObserver<GetPolicyResult> responseObserver) {
            sendScalingPolicyResults(new ArrayList<>(policyMap.values()), responseObserver);
        }

        @Override
        public void getJobScalingPolicies(JobId request, StreamObserver<GetPolicyResult> responseObserver) {
            List<ScalingPolicyResult> scalingPolicyResults = policyMap.values().stream()
                    .filter(p -> p.getJobId().equals(request.getId()))
                    .collect(Collectors.toList());
            sendScalingPolicyResults(scalingPolicyResults, responseObserver);
        }

        @Override
        public void getScalingPolicy(ScalingPolicyID request, StreamObserver<GetPolicyResult> responseObserver) {
            List<ScalingPolicyResult> result = policyMap.values().stream()
                    .filter(p -> p.getId().getId().equals(request.getId())).collect(Collectors.toList());
            sendScalingPolicyResults(result, responseObserver);
        }

        @Override
        public void setAutoScalingPolicy(PutPolicyRequest request, StreamObserver<ScalingPolicyID> responseObserver) {
            ScalingPolicyID newPolicyId = ScalingPolicyID.newBuilder().setId(UUID.randomUUID().toString()).build();
            ScalingPolicyResult newPolicyResult = ScalingPolicyResult.newBuilder().setId(newPolicyId).setJobId(request.getJobId()).build();

            policyMap.put(newPolicyId.getId(), newPolicyResult);
            responseObserver.onNext(newPolicyId);
            responseObserver.onCompleted();
        }

        @Override
        public void updateAutoScalingPolicy(UpdatePolicyRequest request, StreamObserver<Empty> responseObserver) {
            if (policyMap.containsKey(request.getPolicyId().getId())) {
                ScalingPolicyResult currentPolicy = policyMap.get(request.getPolicyId().getId());
                ScalingPolicyResult updatePolicy = ScalingPolicyResult.newBuilder().setId(currentPolicy.getId()).setJobId(currentPolicy.getJobId())
                        .setScalingPolicy(request.getScalingPolicy()).build();
                policyMap.put(currentPolicy.getId().getId(), updatePolicy);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new IllegalArgumentException(request.getPolicyId().getId() + " is Invalid"));
            }
        }

        @Override
        public void deleteAutoScalingPolicy(DeletePolicyRequest request, StreamObserver<Empty> responseObserver) {
            if (policyMap.containsKey(request.getId().getId())) {
                policyMap.remove(request.getId().getId());
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new IllegalArgumentException(request.getId().getId() + " is Invalid"));
            }
        }

        private void sendScalingPolicyResults(List<ScalingPolicyResult> results,
                                              StreamObserver<GetPolicyResult> responseObserver) {
            GetPolicyResult.Builder getPolicyResultBuilder = GetPolicyResult.newBuilder();
            results.forEach(getPolicyResultBuilder::addItems);
            responseObserver.onNext(getPolicyResultBuilder.build());
            responseObserver.onCompleted();
        }
    }

    static class FailingCell extends AutoScalingServiceImplBase {
        @Override
        public void getAllScalingPolicies(Empty request, StreamObserver<GetPolicyResult> responseObserver) {
            responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
        }
    }

}
