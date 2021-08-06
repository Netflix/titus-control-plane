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

package com.netflix.titus.master.integration.v3.appscale;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Empty;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.grpc.protogen.AutoScalingServiceGrpc;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyStatus;
import com.netflix.titus.master.appscale.endpoint.v3.grpc.AutoScalingTestUtils;
import com.netflix.titus.master.appscale.service.AutoScalingPolicyTests;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.grpc.protogen.ScalingPolicyStatus.ScalingPolicyState.Deleted;
import static com.netflix.titus.grpc.protogen.ScalingPolicyStatus.ScalingPolicyState.Deleting;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Category(IntegrationTest.class)
public class AutoScalingGrpcTest extends BaseIntegrationTest {
    private static Logger log = LoggerFactory.getLogger(AutoScalingGrpcTest.class);

    private AutoScalingServiceGrpc.AutoScalingServiceStub client;

    private static final long TIMEOUT_MS = 30_000;

    @Rule
    public final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusCells.basicKubeCell(2));

    @Before
    public void setUp() throws Exception {
        client = titusStackResource.getGateway().getAutoScaleGrpcClient();
    }

    /**
     * Test that we can retrieve a policy by a specific ID.
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void testGetPolicyById() throws Exception {
        String jobId = "Titus-123";

        PutPolicyRequest putPolicyRequest = AutoScalingTestUtils.generatePutPolicyRequest(jobId, PolicyType.StepScaling);
        TestStreamObserver<ScalingPolicyID> putResponse = new TestStreamObserver<>();
        client.setAutoScalingPolicy(putPolicyRequest, putResponse);
        ScalingPolicyID scalingPolicyID = putResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(!scalingPolicyID.getId().isEmpty());
        log.info("Put policy {} with ID {}", putPolicyRequest, scalingPolicyID);

        JobId getPolicyRequest = JobId.newBuilder().setId(jobId).build();
        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getJobScalingPolicies(getPolicyRequest, getResponse);

        GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        log.info("Got result {}", getPolicyResult);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        assertThat(getPolicyResult.getItems(0).getId()).isEqualTo(scalingPolicyID);
        assertThat(getPolicyResult.getItems(0).getJobId()).isEqualTo(jobId);
    }

    /**
     * Test that a policy can be deleted.
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void testDeletePolicyById() throws Exception {
        // Put a policy
        String jobId = "Titus-1";
        PutPolicyRequest putPolicyRequest = AutoScalingTestUtils.generatePutPolicyRequest(jobId, PolicyType.StepScaling);
        TestStreamObserver<ScalingPolicyID> putResponse = new TestStreamObserver<>();
        client.setAutoScalingPolicy(putPolicyRequest, putResponse);
        ScalingPolicyID scalingPolicyID = putResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(!scalingPolicyID.getId().isEmpty());

        // Delete the policy
        TestStreamObserver<Empty> deletePolicyResult = new TestStreamObserver<>();
        DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.newBuilder().setId(scalingPolicyID).build();
        client.deleteAutoScalingPolicy(deletePolicyRequest, deletePolicyResult);
        deletePolicyResult.awaitDone();
        assertThat(deletePolicyResult.hasError()).isFalse();

        // Make sure it's set to Deleting or Deleted state
        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getScalingPolicy(scalingPolicyID, getResponse);

        GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        log.info("Got result {}", getPolicyResult);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        assertThat(isDeletingState(getPolicyResult.getItems(0).getPolicyState()));
    }

    private static boolean isDeletingState(ScalingPolicyStatus status) {
        if (status.getState() == Deleted || status.getState() == Deleting) {
            return true;
        }
        return false;
    }

    /**
     * Test that a non-existent job returns an empty list of policies.
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void testGetNonexistentJob() throws Exception {
        String jobId = "Titus-0";
        JobId getPolicyRequest = JobId.newBuilder().setId(jobId).build();
        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getJobScalingPolicies(getPolicyRequest, getResponse);

        GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        log.info("Got result {}", getPolicyResult);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(0);
    }

    /**
     * Test that a non-exitent policy returns an empty list of policies.
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    @Ignore // GRPC request/response semantics requires that a value is always returned
    public void testGetNonexistentPolicy() throws Exception {
        ScalingPolicyID scalingPolicyID = ScalingPolicyID.newBuilder().setId("deadbeef").build();
        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getScalingPolicy(scalingPolicyID, getResponse);

        getResponse.awaitDone();
        assertThat(getResponse.getEmittedItems().size()).isEqualTo(0);
        assertThat(getResponse.hasError()).isFalse();
    }

    /**
     * Test that we can get multiple exceptions.
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void getAllPolicies() throws Exception {
        Set<ScalingPolicyID> policyIDSet = new HashSet<>();
        int numJobs = 2;
        for (int i = 1; i <= numJobs; i++) {
            PutPolicyRequest putPolicyRequest = AutoScalingTestUtils.generatePutPolicyRequest("Titus-" + i, PolicyType.StepScaling);
            TestStreamObserver<ScalingPolicyID> putResponse = new TestStreamObserver<>();
            client.setAutoScalingPolicy(putPolicyRequest, putResponse);
            ScalingPolicyID scalingPolicyID = putResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertThat(!scalingPolicyID.getId().isEmpty());
            policyIDSet.add(scalingPolicyID);
        }

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getAllScalingPolicies(Empty.newBuilder().build(), getResponse);
        GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(numJobs);
        getPolicyResult.getItemsList().forEach(scalingPolicyResult -> {
            assertThat(policyIDSet.contains(scalingPolicyResult.getId())).isTrue();
        });
    }

    /**
     * Test policy configuration update for target tracking policy
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void testUpdatePolicyConfigurationForTargetTracking() throws Exception {
        String jobId = "Titus-123";
        PutPolicyRequest putPolicyRequest = AutoScalingTestUtils.generatePutPolicyRequest(jobId, PolicyType.TargetTrackingScaling);
        TestStreamObserver<ScalingPolicyID> putResponse = new TestStreamObserver<>();
        client.setAutoScalingPolicy(putPolicyRequest, putResponse);
        ScalingPolicyID scalingPolicyID = putResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(!scalingPolicyID.getId().isEmpty());

        TestStreamObserver<Empty> updateResponse = new TestStreamObserver<>();
        client.updateAutoScalingPolicy(
                AutoScalingTestUtils.generateUpdateTargetTrackingPolicyRequest(scalingPolicyID.getId(), 100.0),
                updateResponse);
        updateResponse.awaitDone();

        AutoScalingPolicyTests.waitForCondition(() -> {
            TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
            client.getScalingPolicy(scalingPolicyID, getResponse);

            try {
                GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return getPolicyResult.getItemsCount() == 1 &&
                        getPolicyResult.getItems(0).getScalingPolicy().getTargetPolicyDescriptor().getTargetValue().getValue() == 100.0;
            } catch (Exception ignored) {
            }
            return false;
        });

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getScalingPolicy(scalingPolicyID, getResponse);
        getResponse.awaitDone();

        GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        DoubleValue targetValue = getPolicyResult.getItems(0).getScalingPolicy().getTargetPolicyDescriptor().getTargetValue();
        assertThat(targetValue.getValue()).isEqualTo(100.0);
    }

    /**
     * Test policy configuration update for target tracking policy
     *
     * @throws Exception
     */
    @Test(timeout = TEST_TIMEOUT_MS)
    public void testUpdatePolicyConfigurationForStepScaling() throws Exception {
        String jobId = "Titus-123";
        PutPolicyRequest putPolicyRequest = AutoScalingTestUtils.generatePutPolicyRequest(jobId, PolicyType.StepScaling);
        TestStreamObserver<ScalingPolicyID> putResponse = new TestStreamObserver<>();
        client.setAutoScalingPolicy(putPolicyRequest, putResponse);
        putResponse.awaitDone();
        ScalingPolicyID scalingPolicyID = putResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(!scalingPolicyID.getId().isEmpty());

        TestStreamObserver<Empty> updateResponse = new TestStreamObserver<>();
        client.updateAutoScalingPolicy(
                AutoScalingTestUtils.generateUpdateStepScalingPolicyRequest(scalingPolicyID.getId(), 100.0),
                updateResponse);
        updateResponse.awaitDone();

        AutoScalingPolicyTests.waitForCondition(() -> {
            TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
            client.getScalingPolicy(scalingPolicyID, getResponse);
            try {
                GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return getPolicyResult.getItemsCount() == 1 &&
                        getPolicyResult.getItems(0).getScalingPolicy().getStepPolicyDescriptor().getAlarmConfig().getThreshold().getValue() == 100.0;
            } catch (Exception ignored) {
            }
            return false;
        });

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        client.getScalingPolicy(scalingPolicyID, getResponse);
        getResponse.awaitDone();

        GetPolicyResult getPolicyResult = getResponse.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        DoubleValue threshold = getPolicyResult.getItems(0).getScalingPolicy().getStepPolicyDescriptor().getAlarmConfig().getThreshold();
        assertThat(threshold.getValue()).isEqualTo(100.0);
    }
}
