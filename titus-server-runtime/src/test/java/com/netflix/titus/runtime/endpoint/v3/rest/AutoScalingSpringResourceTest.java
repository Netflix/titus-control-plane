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

package com.netflix.titus.runtime.endpoint.v3.rest;

import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import com.netflix.titus.runtime.service.AutoScalingService;
import com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import rx.Completable;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebMvcTest()
@ContextConfiguration(classes = {AutoScalingSpringResource.class, ProtobufHttpMessageConverter.class})
public class AutoScalingSpringResourceTest {

    private static final GetPolicyResult GET_POLICY_RESULT = GetPolicyResult.newBuilder().build();

    private static final ScalingPolicyID SCALING_POLICY_ID = ScalingPolicyID.newBuilder().setId("myPolicyId").build();

    @MockBean
    private AutoScalingService serviceMock;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetAllScalingPolicies() throws Exception {
        when(serviceMock.getAllScalingPolicies()).thenReturn(Observable.just(GET_POLICY_RESULT));
        GetPolicyResult entity = SpringMockMvcUtil.doGet(mockMvc, "/api/v3/autoscaling/scalingPolicies", GetPolicyResult.class);
        assertThat(entity).isEqualTo(GET_POLICY_RESULT);

        verify(serviceMock, times(1)).getAllScalingPolicies();
    }

    @Test
    public void testGetScalingPolicyForJob() throws Exception {
        JobId jobId = JobId.newBuilder().setId("myJobId").build();
        when(serviceMock.getJobScalingPolicies(jobId)).thenReturn(Observable.just(GET_POLICY_RESULT));
        GetPolicyResult entity = SpringMockMvcUtil.doGet(mockMvc, String.format("/api/v3/autoscaling/scalingPolicies/%s", jobId.getId()), GetPolicyResult.class);
        assertThat(entity).isEqualTo(GET_POLICY_RESULT);

        verify(serviceMock, times(1)).getJobScalingPolicies(jobId);
    }

    @Test
    public void testSetScalingPolicy() throws Exception {
        PutPolicyRequest request = PutPolicyRequest.newBuilder().build();

        when(serviceMock.setAutoScalingPolicy(request)).thenReturn(Observable.just(SCALING_POLICY_ID));
        ScalingPolicyID entity = SpringMockMvcUtil.doPost(mockMvc, "/api/v3/autoscaling/scalingPolicy", request, ScalingPolicyID.class);
        assertThat(entity).isEqualTo(SCALING_POLICY_ID);

        verify(serviceMock, times(1)).setAutoScalingPolicy(request);
    }

    @Test
    public void testGetScalingPolicy() throws Exception {
        when(serviceMock.getScalingPolicy(SCALING_POLICY_ID)).thenReturn(Observable.just(GET_POLICY_RESULT));
        GetPolicyResult entity = SpringMockMvcUtil.doGet(mockMvc, String.format("/api/v3/autoscaling/scalingPolicy/%s", SCALING_POLICY_ID.getId()), GetPolicyResult.class);
        assertThat(entity).isEqualTo(GET_POLICY_RESULT);

        verify(serviceMock, times(1)).getScalingPolicy(SCALING_POLICY_ID);
    }

    @Test
    public void testRemovePolicy() throws Exception {
        DeletePolicyRequest request = DeletePolicyRequest.newBuilder().setId(SCALING_POLICY_ID).build();

        when(serviceMock.deleteAutoScalingPolicy(request)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doDelete(mockMvc, String.format("/api/v3/autoscaling/scalingPolicy/%s", SCALING_POLICY_ID.getId()));

        verify(serviceMock, times(1)).deleteAutoScalingPolicy(request);
    }

    @Test
    public void testUpdateScalingPolicy() throws Exception {
        UpdatePolicyRequest request = UpdatePolicyRequest.newBuilder().build();

        when(serviceMock.updateAutoScalingPolicy(request)).thenReturn(Completable.complete());
        SpringMockMvcUtil.doPut(mockMvc, "/api/v3/autoscaling/scalingPolicy", request);

        verify(serviceMock, times(1)).updateAutoScalingPolicy(request);

    }
}