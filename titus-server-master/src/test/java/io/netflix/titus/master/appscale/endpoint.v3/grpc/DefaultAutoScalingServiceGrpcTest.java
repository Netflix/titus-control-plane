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

package io.netflix.titus.master.appscale.endpoint.v3.grpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.Empty;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import io.netflix.titus.api.appscale.model.PolicyType;
import io.netflix.titus.api.appscale.service.AppScaleManager;
import io.netflix.titus.api.appscale.store.AppScalePolicyStore;
import io.netflix.titus.master.appscale.service.AutoScalingPolicyTests;
import io.netflix.titus.master.appscale.service.DefaultAppScaleManager;
import io.netflix.titus.runtime.store.v3.memory.InMemoryPolicyStore;
import io.netflix.titus.testkit.grpc.TestStreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.schedulers.Schedulers;

import static com.netflix.titus.grpc.protogen.ScalingPolicyStatus.ScalingPolicyState.Applied;
import static com.netflix.titus.grpc.protogen.ScalingPolicyStatus.ScalingPolicyState.Deleted;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DefaultAutoScalingServiceGrpcTest {
    private static Logger log = LoggerFactory.getLogger(DefaultAutoScalingServiceGrpcTest.class);

    private final AppScalePolicyStore appScalePolicyStore = new InMemoryPolicyStore();
    private final AppScaleManager appScaleManager = new DefaultAppScaleManager(appScalePolicyStore,
            new AutoScalingPolicyTests.MockAlarmClient(),
            new AutoScalingPolicyTests.MockAppAutoScalingClient(), null, null, null,
            new DefaultRegistry(),
            AutoScalingPolicyTests.mockAppScaleManagerConfiguration(), Schedulers.immediate());
    private final DefaultAutoScalingServiceGrpc service = new DefaultAutoScalingServiceGrpc(appScaleManager);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Tests setting and getting a Target Tracking Policy.
     *
     * @throws Exception
     */
    @Test
    public void testTargetTrackingPolicy() throws Exception {
        String jobId = "Titus-123";

        ScalingPolicyID scalingPolicyID = putPolicyWithJobId(jobId, PolicyType.TargetTrackingScaling);

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        service.getScalingPolicy(scalingPolicyID, getResponse);
        GetPolicyResult getPolicyResult = getResponse.takeNext();
        log.info("Got response {}", getPolicyResult);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        assertThat(getPolicyResult.getItems(0).getId()).isEqualTo(scalingPolicyID);
        assertThat(getPolicyResult.getItems(0).getJobId()).isEqualTo(jobId);
    }

    /**
     * Tests setting and getting policies by Job ID.
     *
     * @throws Exception
     */
    @Test
    public void testSetAndGetJobScalingPolicy() throws Exception {
        int numJobs = 2;
        int numPoliciesPerJob = 3;
        Map<String, Set<ScalingPolicyID>> jobIdToPoliciesMap = putPoliciesPerJob(numJobs, numPoliciesPerJob, PolicyType.StepScaling);

        // For each job, check that all of the policies exist
        jobIdToPoliciesMap.forEach((jobId, policyIDSet) -> {
            TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
            JobId jobIdRequest = JobId.newBuilder().setId(jobId).build();
            service.getJobScalingPolicies(jobIdRequest, getResponse);

            GetPolicyResult getPolicyResult = getResponse.takeNext();
            assertThat(getPolicyResult.getItemsCount()).isEqualTo(numPoliciesPerJob);
            for (int i = 0; i < getPolicyResult.getItemsCount(); i++) {
                ScalingPolicyResult scalingPolicyResult = getPolicyResult.getItems(i);
                assertThat(policyIDSet.contains(scalingPolicyResult.getId())).isTrue();
            }
        });
    }

    /**
     * Tests getting policies by Ref ID.
     *
     * @throws Exception
     */
    @Test
    public void testGetByPolicyId() throws Exception {
        String jobId = "Titus-123";

        ScalingPolicyID scalingPolicyID = putPolicyWithJobId(jobId, PolicyType.StepScaling);

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        service.getScalingPolicy(scalingPolicyID, getResponse);
        GetPolicyResult getPolicyResult = getResponse.takeNext();
        log.info("Got response {}", getPolicyResult);
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        assertThat(getPolicyResult.getItems(0).getId()).isEqualTo(scalingPolicyID);
        assertThat(getPolicyResult.getItems(0).getJobId()).isEqualTo(jobId);
    }

    /**
     * Test getting all policies across multiple jobs
     *
     * @throws Exception
     */
    @Test
    public void testGetAllPolicies() throws Exception {
        int numJobs = 20;
        int numPoliciesPerJob = 5;
        Map<String, Set<ScalingPolicyID>> jobIdToPoliciesMap = putPoliciesPerJob(numJobs, numPoliciesPerJob, PolicyType.StepScaling);

        Empty request = Empty.newBuilder().build();
        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        service.getAllScalingPolicies(request, getResponse);

        GetPolicyResult getPolicyResult = getResponse.takeNext();
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(numJobs * numPoliciesPerJob);
    }

    /**
     * Tests deleting policies by Ref ID.
     *
     * @throws Exception
     */
    @Test
    public void testDeletePolicyId() throws Exception {
        String jobId = "Titus-123";
        ScalingPolicyID scalingPolicyID = putPolicyWithJobId(jobId, PolicyType.StepScaling);

        DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.newBuilder()
                .setId(scalingPolicyID)
                .build();
        TestStreamObserver<Empty> deleteResponse = new TestStreamObserver<>();
        service.deleteAutoScalingPolicy(deletePolicyRequest, deleteResponse);
        deleteResponse.awaitDone();

        AutoScalingPolicyTests.waitForCondition(() -> {
            TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
            service.getScalingPolicy(scalingPolicyID, getResponse);
            GetPolicyResult getPolicyResult = getResponse.takeNext();
            return getPolicyResult.getItemsCount() == 1 &&
                    getPolicyResult.getItems(0).getPolicyState().getState() == Deleted;
        });

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        service.getScalingPolicy(scalingPolicyID, getResponse);
        GetPolicyResult getPolicyResult = getResponse.takeNext();
        // Check that the policy still exists but the state is updated
        assertThat(getPolicyResult.getItemsCount()).isEqualTo(1);
        assertThat(getPolicyResult.getItems(0).getPolicyState().getState()).isEqualTo(Deleted);
    }

    @Test
    public void testUpdatePolicyConfigurationForTargetTracking() throws Exception {
        ScalingPolicyID policyId = putPolicyWithJobId("Job-1", PolicyType.TargetTrackingScaling);
        TestStreamObserver<Empty> updateResponse = new TestStreamObserver<>();
        service.updateAutoScalingPolicy(
                AutoScalingTestUtils.generateUpdateTargetTrackingPolicyRequest(policyId.getId(), 100.0),
                updateResponse);

        AutoScalingPolicyTests.waitForCondition(() -> {
            TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
            service.getScalingPolicy(policyId, getResponse);
            GetPolicyResult getPolicyResult = getResponse.takeNext();
            return getPolicyResult.getItems(0).getScalingPolicy().getTargetPolicyDescriptor().getTargetValue().getValue() == 100.0 &&
                    getPolicyResult.getItems(0).getPolicyState().getState() == Applied;

        });

        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        service.getScalingPolicy(policyId, getResponse);
        GetPolicyResult getPolicyResult = getResponse.takeNext();
        assertThat(getPolicyResult.getItems(0).getScalingPolicy().getTargetPolicyDescriptor().getTargetValue().getValue()).isEqualTo(100.0);
        assertThat(getPolicyResult.getItems(0).getPolicyState().getState()).isEqualTo(Applied);
    }


    @Test
    public void testUpdatePolicyConfigurationForStepScaling() throws Exception {
        ScalingPolicyID policyId = putPolicyWithJobId("Job-1", PolicyType.StepScaling);
        TestStreamObserver<Empty> updateResponse = new TestStreamObserver<>();
        service.updateAutoScalingPolicy(
                AutoScalingTestUtils.generateUpdateStepScalingPolicyRequest(policyId.getId(), 100.0),
                updateResponse);
        updateResponse.awaitDone();

        AutoScalingPolicyTests.waitForCondition(() -> {
            TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
            service.getScalingPolicy(policyId, getResponse);
            GetPolicyResult getPolicyResult = getResponse.takeNext();
            return getPolicyResult.getItems(0).getScalingPolicy().getStepPolicyDescriptor().getAlarmConfig().getThreshold().getValue() == 100.0 &&
                    getPolicyResult.getItems(0).getPolicyState().getState() == Applied;

        });
        TestStreamObserver<GetPolicyResult> getResponse = new TestStreamObserver<>();
        service.getScalingPolicy(policyId, getResponse);
        GetPolicyResult getPolicyResult = getResponse.takeNext();
        assertThat(getPolicyResult.getItems(0).getScalingPolicy().getStepPolicyDescriptor().getAlarmConfig().getThreshold().getValue()).isEqualTo(100.0);
        assertThat(getPolicyResult.getItems(0).getPolicyState().getState()).isEqualTo(Applied);
    }

    /**
     * Puts multiple a specified number of policies for given jobs.
     *
     * @return
     */
    private Map<String, Set<ScalingPolicyID>> putPoliciesPerJob(int numJobs, int numPoliciesPerJob, PolicyType policyType) {
        // Create job entries
        Map<String, Set<ScalingPolicyID>> jobIdToPoliciesMap = new ConcurrentHashMap<>();
        for (int i = 1; i <= numJobs; i++) {
            jobIdToPoliciesMap.put("Titus-" + i, new HashSet<>());
        }

        // For each job, insert policies
        jobIdToPoliciesMap.forEach((jobId, policyIDSet) -> {
            for (int i = 0; i < numPoliciesPerJob; i++) {
                policyIDSet.add(putPolicyWithJobId(jobId, policyType));
            }
        });
        return jobIdToPoliciesMap;
    }

    private ScalingPolicyID putPolicyWithJobId(String jobId, PolicyType policyType) {
        PutPolicyRequest putPolicyRequest = AutoScalingTestUtils.generatePutPolicyRequest(jobId, policyType);
        TestStreamObserver<ScalingPolicyID> putResponse = new TestStreamObserver<>();
        service.setAutoScalingPolicy(putPolicyRequest, putResponse);
        log.info("Put policy {}", putPolicyRequest);

        ScalingPolicyID scalingPolicyID = putResponse.takeNext();
        assertThat(scalingPolicyID.getId()).isNotEmpty();
        return scalingPolicyID;
    }
}
