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

import java.util.Optional;

import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.service.LoadBalancerService;
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

import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.JUNIT_REST_CALL_METADATA;
import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.NEXT_PAGE_OF_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebMvcTest
@ContextConfiguration(classes = {LoadBalancerSpringResource.class, ProtobufHttpMessageConverter.class})
public class LoadBalancerSpringResourceTest {

    private static final String JOB_ID = "myJobId";
    private static final String LOAD_BALANCER_ID = "lb1";

    private static final GetJobLoadBalancersResult GET_JOB_LOAD_BALANCERS_RESULT = GetJobLoadBalancersResult.newBuilder()
            .setJobId(JOB_ID)
            .addLoadBalancers(LoadBalancerId.newBuilder().setId(LOAD_BALANCER_ID).build())
            .build();

    @MockBean
    private LoadBalancerService serviceMock;

    @MockBean
    private SystemLogService systemLogService;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetJobLoadBalancers() throws Exception {
        JobId jobIdWrapper = JobId.newBuilder().setId(JOB_ID).build();
        when(serviceMock.getLoadBalancers(jobIdWrapper, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(GET_JOB_LOAD_BALANCERS_RESULT));
        GetJobLoadBalancersResult entity = SpringMockMvcUtil.doGet(mockMvc, String.format("/api/v3/loadBalancers/%s", JOB_ID), GetJobLoadBalancersResult.class);
        assertThat(entity).isEqualTo(GET_JOB_LOAD_BALANCERS_RESULT);

        verify(serviceMock, times(1)).getLoadBalancers(jobIdWrapper, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testGetAllLoadBalancers() throws Exception {
        GetAllLoadBalancersRequest request = GetAllLoadBalancersRequest.newBuilder().setPage(NEXT_PAGE_OF_1).build();
        GetAllLoadBalancersResult response = GetAllLoadBalancersResult.newBuilder()
                .setPagination(SpringMockMvcUtil.paginationOf(NEXT_PAGE_OF_1))
                .addJobLoadBalancers(GET_JOB_LOAD_BALANCERS_RESULT)
                .build();
        when(serviceMock.getAllLoadBalancers(request, JUNIT_REST_CALL_METADATA)).thenReturn(Observable.just(response));

        GetAllLoadBalancersResult entity = SpringMockMvcUtil.doPaginatedGet(mockMvc, "/api/v3/loadBalancers", GetAllLoadBalancersResult.class, NEXT_PAGE_OF_1);
        assertThat(entity).isEqualTo(response);

        verify(serviceMock, times(1)).getAllLoadBalancers(request, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testAddLoadBalancer() throws Exception {
        AddLoadBalancerRequest forwardedRequest = AddLoadBalancerRequest.newBuilder()
                .setJobId(JOB_ID)
                .setLoadBalancerId(LoadBalancerId.newBuilder().setId(LOAD_BALANCER_ID).build())
                .build();
        when(serviceMock.addLoadBalancer(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());

        SpringMockMvcUtil.doPost(
                mockMvc,
                "/api/v3/loadBalancers",
                Optional.empty(),
                Optional.empty(),
                Optional.of(new String[]{"jobId", JOB_ID, "loadBalancerId", LOAD_BALANCER_ID})
        );

        verify(serviceMock, times(1)).addLoadBalancer(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }

    @Test
    public void testRemoveLoadBalancer() throws Exception {
        RemoveLoadBalancerRequest forwardedRequest = RemoveLoadBalancerRequest.newBuilder()
                .setJobId(JOB_ID)
                .setLoadBalancerId(LoadBalancerId.newBuilder().setId(LOAD_BALANCER_ID).build())
                .build();
        when(serviceMock.removeLoadBalancer(forwardedRequest, JUNIT_REST_CALL_METADATA)).thenReturn(Completable.complete());

        SpringMockMvcUtil.doDelete(
                mockMvc,
                "/api/v3/loadBalancers",
                "jobId", JOB_ID, "loadBalancerId", LOAD_BALANCER_ID
        );

        verify(serviceMock, times(1)).removeLoadBalancer(forwardedRequest, JUNIT_REST_CALL_METADATA);
    }
}