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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.Response;

import com.google.protobuf.DoubleValue;
import com.netflix.titus.federation.endpoint.rest.AutoScalingResource;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicy;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.ScalingPolicyResult;
import com.netflix.titus.grpc.protogen.TargetTrackingPolicyDescriptor;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.endpoint.common.rest.RestException;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;


public class AutoScalingResourceTest extends AggregatingAutoScalingTestBase {

    @Test
    public void getPoliciesFromTwoCells() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyID policy2 = ScalingPolicyID.newBuilder().setId(POLICY_2).build();
        List<ScalingPolicyID> policyIds = Arrays.asList(policy1, policy2);

        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).build();
        ScalingPolicyResult policyTwoResult = ScalingPolicyResult.newBuilder().setId(policy2).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithPolicies cellTwoService = new CellWithPolicies(Collections.singletonList(policyTwoResult));

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(cellTwoService);

        final AutoScalingResource autoScalingResource = new AutoScalingResource(service);
        final GetPolicyResult allScalingPolicies = autoScalingResource.getAllScalingPolicies();
        assertThat(allScalingPolicies).isNotNull();
        assertThat(allScalingPolicies.getItemsCount()).isEqualTo(2);
        allScalingPolicies.getItemsList()
                .forEach(scalingPolicyResult -> assertThat(policyIds.contains(scalingPolicyResult.getId())).isTrue());
    }

    @Test(expected = RestException.class)
    public void getPoliciesFromTwoCellsWithOneFailing() {
        ScalingPolicyID policy1 = ScalingPolicyID.newBuilder().setId(POLICY_1).build();
        ScalingPolicyResult policyOneResult = ScalingPolicyResult.newBuilder().setId(policy1).build();

        CellWithPolicies cellOneService = new CellWithPolicies(Collections.singletonList(policyOneResult));
        CellWithFailingAutoscalingService badCell = new CellWithFailingAutoscalingService();

        cellOne.getServiceRegistry().addService(cellOneService);
        cellTwo.getServiceRegistry().addService(badCell);

        final AutoScalingResource autoScalingResource = new AutoScalingResource(service);
        autoScalingResource.getAllScalingPolicies();
        assertThat(false).isTrue();
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

        final AutoScalingResource autoScalingResource = new AutoScalingResource(service);
        final GetPolicyResult scalingPolicyForJob = autoScalingResource.getScalingPolicyForJob(JOB_2);
        assertThat(scalingPolicyForJob).isNotNull();
        assertThat(scalingPolicyForJob.getItemsCount()).isEqualTo(1);
        assertThat(scalingPolicyForJob.getItems(0).getJobId()).isEqualTo(JOB_2);
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

        final AutoScalingResource autoScalingResource = new AutoScalingResource(service);
        final GetPolicyResult scalingPolicy = autoScalingResource.getScalingPolicy(POLICY_2);
        assertThat(scalingPolicy.getItemsCount()).isEqualTo(1);
        assertThat(scalingPolicy.getItems(0).getId().getId()).isEqualTo(POLICY_2);

        // bad id
        final GetPolicyResult badScalingPolicyResult = autoScalingResource.getScalingPolicy("badPolicyId");
        assertThat(badScalingPolicyResult.getItemsCount()).isEqualTo(0);
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

        final AutoScalingResource autoScalingResource = new AutoScalingResource(service);
        final ScalingPolicyID scalingPolicyID = autoScalingResource.setScalingPolicy(PutPolicyRequest.newBuilder().setJobId(JOB_2).build());

        assertThat(scalingPolicyID).isNotNull();
        assertThat(scalingPolicyID.getId()).isNotEmpty();

        final GetPolicyResult scalingPolicyForJob = autoScalingResource.getScalingPolicyForJob(JOB_2);
        assertThat(scalingPolicyForJob).isNotNull();
        assertThat(scalingPolicyForJob.getItemsCount()).isEqualTo(2);
        assertThat(scalingPolicyForJob.getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(scalingPolicyForJob.getItems(1).getJobId()).isEqualTo(JOB_2);

        UpdatePolicyRequest updatePolicyRequest = UpdatePolicyRequest.newBuilder()
                .setPolicyId(ScalingPolicyID.newBuilder().setId(POLICY_2))
                .setScalingPolicy(ScalingPolicy.newBuilder()
                        .setTargetPolicyDescriptor(TargetTrackingPolicyDescriptor.newBuilder()
                                .setTargetValue(DoubleValue.newBuilder().setValue(100.0).build()).build()).build()).build();

        final Response updateScalingPolicyResponse = autoScalingResource.updateScalingPolicy(updatePolicyRequest);
        assertThat(updateScalingPolicyResponse.getStatus()).isEqualTo(200);

        final GetPolicyResult updatedScalingPolicy = autoScalingResource.getScalingPolicy(POLICY_2);
        assertThat(updatedScalingPolicy.getItemsCount()).isEqualTo(1);
        assertThat(updatedScalingPolicy.getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(updatedScalingPolicy.getItems(0).getId().getId()).isEqualTo(POLICY_2);
        final DoubleValue targetValue = updatedScalingPolicy.getItems(0).getScalingPolicy().getTargetPolicyDescriptor().getTargetValue();
        assertThat(targetValue.getValue()).isEqualTo(100.0);
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
        cellOne.getServiceRegistry().addService(cellOneJobsService);
        cellTwo.getServiceRegistry().addService(cellTwoService);
        cellTwo.getServiceRegistry().addService(cellTwoJobsService);

        final AutoScalingResource autoScalingResource = new AutoScalingResource(service);
        final ScalingPolicyID scalingPolicyId = autoScalingResource.setScalingPolicy(PutPolicyRequest.newBuilder().setJobId(JOB_2).build());

        assertThat(scalingPolicyId).isNotNull();
        assertThat(scalingPolicyId.getId()).isNotEmpty();

        final GetPolicyResult scalingPolicyForJob = autoScalingResource.getScalingPolicyForJob(JOB_2);
        assertThat(scalingPolicyForJob).isNotNull();
        assertThat(scalingPolicyForJob.getItemsCount()).isEqualTo(2);
        assertThat(scalingPolicyForJob.getItems(0).getJobId()).isEqualTo(JOB_2);
        assertThat(scalingPolicyForJob.getItems(1).getJobId()).isEqualTo(JOB_2);

        final Response deleteResponse = autoScalingResource.removePolicy(scalingPolicyId.getId());
        assertThat(deleteResponse.getStatus()).isEqualTo(200);

        final GetPolicyResult scalingPolicyForJobResult = autoScalingResource.getScalingPolicyForJob(JOB_2);
        assertThat(scalingPolicyForJobResult).isNotNull();
        assertThat(scalingPolicyForJobResult.getItemsCount()).isEqualTo(1);
        assertThat(scalingPolicyForJobResult.getItems(0).getJobId()).isEqualTo(JOB_2);
    }


}
