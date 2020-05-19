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

package com.netflix.titus.federation.endpoint.rest;

import com.netflix.titus.federation.service.AggregatingSchedulerService;
import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
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
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebMvcTest()
@ContextConfiguration(classes = {FederationSchedulerSpringResource.class, ProtobufHttpMessageConverter.class})
public class FederationSchedulerSpringResourceTest {

    private static final SchedulingResultEvent EVENT = SchedulingResultEvent.newBuilder()
            .setSuccess(SchedulingResultEvent.Success.newBuilder().setMessage("Success!").build())
            .build();

    @MockBean
    protected AggregatingSchedulerService serviceMock;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testFindLastSchedulingResult() throws Exception {
        when(serviceMock.findLastSchedulingResult("taskAbc")).thenReturn(Mono.just(EVENT));

        SchedulingResultEvent entity = SpringMockMvcUtil.doGet(mockMvc, "/api/v3/scheduler/results/taskAbc", SchedulingResultEvent.class);
        assertThat(entity).isEqualTo(EVENT);
    }
}