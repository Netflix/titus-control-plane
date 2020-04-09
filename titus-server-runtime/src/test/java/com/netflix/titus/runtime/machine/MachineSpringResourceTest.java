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

package com.netflix.titus.runtime.machine;

import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineQueryResult;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import com.netflix.titus.runtime.connector.machine.ReactorMachineServiceStub;
import com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import reactor.core.publisher.Mono;

import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.NEXT_PAGE_OF_1;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebMvcTest()
@ContextConfiguration(classes = {MachineSpringResource.class, ProtobufHttpMessageConverter.class})
public class MachineSpringResourceTest {

    private static final Machine MACHINE_1 = Machine.newBuilder().setId("machine1").build();
    private static final Machine MACHINE_2 = Machine.newBuilder().setId("machine2").build();

    @MockBean
    protected ReactorMachineServiceStub serviceMock;

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void tetGetMachines() throws Exception {
        MachineQueryResult mockResponse = MachineQueryResult.newBuilder()
                .setPagination(SpringMockMvcUtil.paginationOf(NEXT_PAGE_OF_1))
                .addAllItems(asList(MACHINE_1, MACHINE_2))
                .build();

        QueryRequest stubRequest = QueryRequest.newBuilder()
                .setPage(NEXT_PAGE_OF_1)
                .build();
        when(serviceMock.getMachines(stubRequest)).thenReturn(Mono.just(mockResponse));

        MachineQueryResult entity = SpringMockMvcUtil.doPaginatedGet(mockMvc, "/api/v4/machines", MachineQueryResult.class, NEXT_PAGE_OF_1);
        assertThat(entity).isEqualTo(mockResponse);
    }

    @Test
    public void testGetMachine() throws Exception {
        when(serviceMock.getMachine(ArgumentMatchers.any())).thenReturn(Mono.just(MACHINE_1));

        Machine entity = SpringMockMvcUtil.doGet(mockMvc, "/api/v4/machines/" + MACHINE_1.getId(), Machine.class);
        assertThat(entity).isEqualTo(MACHINE_1);
    }
}