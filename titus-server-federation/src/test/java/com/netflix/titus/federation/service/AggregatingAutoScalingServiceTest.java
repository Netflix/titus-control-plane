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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.DoubleValue;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicy;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import com.netflix.titus.grpc.protogen.TargetTrackingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;
import rx.observers.AssertableSubscriber;

import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.JUNIT_REST_CALL_METADATA;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class AggregatingAutoScalingServiceTest extends AggregatingAutoScalingTestBase {

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

        final AssertableSubscriber<GetPolicyResult> testSubscriber = service.getAllScalingPolicies(JUNIT_REST_CALL_METADATA).test();
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
        CellWithFailingAutoscalingService badCell = new CellWithFailingAutoscalingService();

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(badCell);

        final AssertableSubscriber<GetPolicyResult> testSubscriber = service.getAllScalingPolicies(JUNIT_REST_CALL_METADATA).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoValues();
        testSubscriber.assertError(StatusRuntimeException.class);
        List<Throwable> onErrorEvents = testSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents.size()).isEqualTo(1);
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

        AssertableSubscriber<GetPolicyResult> testSubscriber = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();

        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);

        // Bad policy id, currently each Cell returns an empty result
        testSubscriber = service.getJobScalingPolicies(JobId.newBuilder().setId("badID").build(), JUNIT_REST_CALL_METADATA).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(0);
    }

    @Test
    public void createAndUpdatePolicyForJobIdFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).setJobId(JOB_1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).setJobId(JOB_2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));

        CellWithJobIds cellOneJobsService = new CellWithJobIds(Collections.singletonList(JOB_1));
        CellWithJobIds cellTwoJobsService = new CellWithJobIds(Collections.singletonList(JOB_2));
        cellOne.getServiceRegistry().addService(cellOneService);
        cellOne.getServiceRegistry().addService(cellOneJobsService);
        cellTwo.getServiceRegistry().addService(cellTwoService);
        cellTwo.getServiceRegistry().addService(cellTwoJobsService);

        AssertableSubscriber<ScalingPolicyID> testSubscriber = service.setAutoScalingPolicy(PutPolicyRequest.newBuilder().setJobId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();
        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();

        List<ScalingPolicyID> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getId()).isNotEmpty();

        AssertableSubscriber<GetPolicyResult> testSubscriber2 = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();
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

        AssertableSubscriber<Void> testSubscriber3 = service.updateAutoScalingPolicy(updatePolicyRequest, JUNIT_REST_CALL_METADATA).test();
        testSubscriber3.assertNoErrors();

        AssertableSubscriber<GetPolicyResult> testSubscriber4 = service.getScalingPolicy(ScalingPolicyID.newBuilder().setId(POLICY_2).build(), JUNIT_REST_CALL_METADATA).test();
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
        CellWithJobIds cellOneJobsService = new CellWithJobIds(Collections.singletonList(JOB_1));
        CellWithJobIds cellTwoJobsService = new CellWithJobIds(Collections.singletonList(JOB_2));

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellOneJobsService);
        cellTwo.getServiceRegistry().addService(cellTwoService);
        cellTwo.getServiceRegistry().addService(cellTwoJobsService);

        AssertableSubscriber<ScalingPolicyID> testSubscriber = service.setAutoScalingPolicy(PutPolicyRequest.newBuilder().setJobId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();
        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        assertThat(testSubscriber.getOnErrorEvents().isEmpty()).isTrue();

        List<ScalingPolicyID> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getId()).isNotEmpty();

        String newPolicyId = onNextEvents.get(0).getId();
        AssertableSubscriber<GetPolicyResult> testSubscriber2 = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();
        testSubscriber2.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents1 = testSubscriber2.getOnNextEvents();
        assertThat(onNextEvents1).isNotNull();
        assertThat(onNextEvents1.size()).isEqualTo(1);
        assertThat(onNextEvents1.get(0).getItemsCount()).isEqualTo(2);
        assertThat(onNextEvents1.get(0).getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(onNextEvents1.get(0).getItems(1).getJobId()).isEqualTo(JOB_2);

        DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.newBuilder().setId(ScalingPolicyID.newBuilder().setId(newPolicyId).build()).build();
        AssertableSubscriber<Void> testSubscriber3 = service.deleteAutoScalingPolicy(deletePolicyRequest, JUNIT_REST_CALL_METADATA).test();
        testSubscriber3.assertNoErrors();

        AssertableSubscriber<GetPolicyResult> testSubscriber4 = service.getJobScalingPolicies(JobId.newBuilder().setId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();
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
                ScalingPolicyID.newBuilder().setId(POLICY_2).build(), JUNIT_REST_CALL_METADATA).test();

        testSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);

        List<GetPolicyResult> onNextEvents = testSubscriber.getOnNextEvents();
        assertThat(onNextEvents).isNotNull();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getItemsCount()).isEqualTo(1);

        // Bad id. The current behavior is "INTERNAL: Completed without a response", but it will change to NOT_FOUND someday
        testSubscriber = service.getScalingPolicy(ScalingPolicyID.newBuilder().setId("badID").build(), JUNIT_REST_CALL_METADATA).test();
        testSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        testSubscriber.assertError(StatusRuntimeException.class);
        testSubscriber.assertNoValues();
        List<Throwable> onErrorEvents = testSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents).isNotNull();
        assertThat(onErrorEvents).hasSize(1);
        assertThat(Status.fromThrowable(onErrorEvents.get(0)).getCode()).isEqualTo(Status.CANCELLED.getCode());
    }
}
