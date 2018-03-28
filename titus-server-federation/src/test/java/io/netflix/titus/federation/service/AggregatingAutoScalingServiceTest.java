/*
 * Copyright 2017 Netflix, Inc.
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
package io.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicy;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import com.netflix.titus.grpc.protogen.TargetTrackingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import io.netflix.titus.api.federation.model.Cell;
import io.netflix.titus.common.grpc.AnonymousSessionContext;
import io.netflix.titus.federation.startup.GrpcConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.observers.AssertableSubscriber;

import static com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc.AutoScalingServiceImplBase;
import static com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceImplBase;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AggregatingAutoScalingServiceTest {
    static final String POLICY_1 = "policy1";
    static final String POLICY_2 = "policy2";
    static final String JOB_1 = "job1";
    static final String JOB_2 = "job2";

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();


    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();

    private AggregatingAutoScalingService service;

    @Before
    public void setUp() {
        CellConnector connector = mock(CellConnector.class);
        Map<Cell, ManagedChannel> cellMap = new HashMap<>();
        cellMap.put(new Cell("one", "1"), cellOne.getChannel());
        cellMap.put(new Cell("two", "2"), cellTwo.getChannel());
        when(connector.getChannels()).thenReturn(cellMap);

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(1000L);
        service = new AggregatingAutoScalingService(connector, new AnonymousSessionContext(), grpcConfiguration);
    }


    @Test
    public void getPoliciesFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        final AssertableSubscriber<GetPolicyResult> testSubscriber = service.getAllScalingPolicies().test();
        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        List<GetPolicyResult> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(2);
    }

    @Test
    public void getPoliciesFromTwoCellsWithOneFailing() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        FailingCell badCell = new FailingCell();

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(badCell);

        final AssertableSubscriber<GetPolicyResult> testSubscriber = service.getAllScalingPolicies().test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents = testSubscriber.getOnNextEvents();
        List<Throwable> onErrorEvents = testSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents.size()).isEqualTo(1);
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(0);
    }


    @Test
    public void getPoliciesForJobFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).setJobId(JOB_1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).setJobId(JOB_2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        AssertableSubscriber<GetPolicyResult> testSubscriber = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build()).test();

        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);
    }

    @Test
    public void createAndUpdatePolicyForJobIdFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).setJobId(JOB_1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).setJobId(JOB_2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));

        CellWithJobs cellOneJobsService = new CellWithJobs(Collections.singletonList(JOB_1));
        CellWithJobs cellTwoJobsService = new CellWithJobs(Collections.singletonList(JOB_2));
        cellOne.getServiceRegistry().addService(cellOneService);
        cellOne.getServiceRegistry().addService(cellOneJobsService);
        cellTwo.getServiceRegistry().addService(cellTwoService);
        cellTwo.getServiceRegistry().addService(cellTwoJobsService);

        AssertableSubscriber<ScalingPolicyID> testSubscriber = service.setAutoScalingPolicy(PutPolicyRequest.newBuilder().setJobId(JOB_2).build()).test();
        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents().isEmpty()).isTrue();

        List<ScalingPolicyID> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getId()).isNotEmpty();

        AssertableSubscriber<GetPolicyResult> testSubscriber2 = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build()).test();
        testSubscriber2.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents1 = testSubscriber2.getOnNextEvents();
        assertThat(onNextEvents1).isNotNull();
        assertThat(onNextEvents1.size()).isEqualTo(1);
        assertThat(onNextEvents1.get(0).getItemsCount()).isEqualTo(2);
        assertThat(onNextEvents1.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(onNextEvents1.get(0).getItems(1).getJobId()).isEqualTo(JOB_2);

        UpdatePolicyRequest updatePolicyRequest = UpdatePolicyRequest.newBuilder()
                .setPolicyId(ScalingPolicyID.newBuilder().setId(POLICY_2))
                .setScalingPolicy(ScalingPolicy.newBuilder()
                        .setTargetPolicyDescriptor(TargetTrackingPolicyDescriptor.newBuilder()
                                .setTargetValue(DoubleValue.newBuilder().setValue(100.0).build()).build()).build()).build();

        AssertableSubscriber<Void> testSubscriber3 = service.updateAutoScalingPolicy(updatePolicyRequest).test();
        List<Throwable> onErrorEvents = testSubscriber3.getOnErrorEvents();
        assertThat(onErrorEvents.size()).isEqualTo(0);

        AssertableSubscriber<GetPolicyResult> testSubscriber4 = service.getScalingPolicy(ScalingPolicyID.newBuilder().setId(POLICY_2).build()).test();
        testSubscriber2.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents2 = testSubscriber4.getOnNextEvents();
        assertThat(onNextEvents2).isNotNull();
        assertThat(onNextEvents2.size()).isEqualTo(1);
        assertThat(onNextEvents2.get(0).getItemsCount()).isEqualTo(1);
        assertThat(onNextEvents2.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(onNextEvents2.get(0).getItems(0).getId().getId()).isEqualTo(POLICY_2);
        ScalingPolicy scalingPolicy = onNextEvents2.get(0).getItems(0).getScalingPolicy();
        double updatedValue = scalingPolicy.getTargetPolicyDescriptor().getTargetValue().getValue();
        assertThat(updatedValue).isEqualTo(100);
    }

    @Test
    public void createAndDeletePolicyForJobIdFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).setJobId(JOB_1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).setJobId(JOB_2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));
        CellWithJobs cellOneJobsService = new CellWithJobs(Collections.singletonList(JOB_1));
        CellWithJobs cellTwoJobsService = new CellWithJobs(Collections.singletonList(JOB_2));

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellOneJobsService);
        cellTwo.getServiceRegistry().addService(cellTwoService);
        cellTwo.getServiceRegistry().addService(cellTwoJobsService);

        AssertableSubscriber<ScalingPolicyID> testSubscriber = service.setAutoScalingPolicy(PutPolicyRequest.newBuilder().setJobId(JOB_2).build()).test();
        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents().isEmpty()).isTrue();

        List<ScalingPolicyID> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getId()).isNotEmpty();

        String newPolicyId = onNextEvents.get(0).getId();
        AssertableSubscriber<GetPolicyResult> testSubscriber2 = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build()).test();
        testSubscriber2.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents1 = testSubscriber2.getOnNextEvents();
        assertThat(onNextEvents1).isNotNull();
        assertThat(onNextEvents1.size()).isEqualTo(1);
        assertThat(onNextEvents1.get(0).getItemsCount()).isEqualTo(2);
        assertThat(onNextEvents1.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(onNextEvents1.get(0).getItems(1).getJobId()).isEqualTo(JOB_2);

        DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.newBuilder().setId(ScalingPolicyID.newBuilder().setId(newPolicyId).build()).build();

        AssertableSubscriber<Void> testSubscriber3 = service.deleteAutoScalingPolicy(deletePolicyRequest).test();
        List<Throwable> onErrorEvents = testSubscriber3.getOnErrorEvents();
        assertThat(onErrorEvents.size()).isEqualTo(0);

        AssertableSubscriber<GetPolicyResult> testSubscriber4 = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build()).test();
        testSubscriber2.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents2 = testSubscriber4.getOnNextEvents();
        assertThat(onNextEvents2).isNotNull();
        assertThat(onNextEvents2.size()).isEqualTo(1);
        assertThat(onNextEvents2.get(0).getItemsCount()).isEqualTo(1);
        assertThat(onNextEvents2.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(onNextEvents2.get(0).getItems(0).getId().getId()).isEqualTo(POLICY_2);
    }

    @Test
    public void getPolicyByIdFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        AssertableSubscriber<GetPolicyResult> testSubscriber = service.getScalingPolicy(
                ScalingPolicyID.newBuilder().setId(POLICY_2).build()).test();

        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(1);

        // Bad policy id
        testSubscriber = service.getScalingPolicy(ScalingPolicyID.newBuilder().setId("badID").build()).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(0);

    }


    private static class CellWithJobs extends JobManagementServiceImplBase {
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

    private static class CellWithPolicies extends AutoScalingServiceImplBase {
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

    private static class FailingCell extends AutoScalingServiceImplBase {
        @Override
        public void getAllScalingPolicies(Empty request, StreamObserver<GetPolicyResult> responseObserver) {
            responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
        }
    }
}
