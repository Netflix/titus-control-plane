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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.master.scheduler.VMOperations;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.web.reactive.server.WebTestClient;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class VmManagementResourceTest {

    private static final ParameterizedTypeReference<List<ServerInfo>> LIST_SERVER_INFOS_TREF = new ParameterizedTypeReference<List<ServerInfo>>() {
    };

    private static final ParameterizedTypeReference<List<VMOperations.AgentInfo>> LIST_AGENT_INFO_TREF = new ParameterizedTypeReference<List<VMOperations.AgentInfo>>() {
    };

    private static final ParameterizedTypeReference<Map<String, List<VMOperations.JobsOnVMStatus>>> MAP_JOBS_ON_VMS_TREF =
            new ParameterizedTypeReference<Map<String, List<VMOperations.JobsOnVMStatus>>>() {
            };

    private static ServerInfoResolver serverInfoResolver = mock(ServerInfoResolver.class);
    private static VMOperations vmOps = mock(VMOperations.class);

    private static final VmManagementResource restService = new VmManagementResource(serverInfoResolver, vmOps);

    @ClassRule
    public static final JaxRsServerResource<VmManagementResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static WebTestClient testClient;

    @BeforeClass
    public static void setUpClass() {
        testClient = WebTestClient.bindToServer()
                .baseUrl(jaxRsServer.getBaseURI() + VmManagementEndpoint.PATH_API_V2_VM + '/')
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .build();
    }

    @Before
    public void setUp() throws Exception {
        Mockito.reset(serverInfoResolver, vmOps);
    }

    @Test
    public void testGetServerInfos() {
        when(serverInfoResolver.resolveAll()).thenReturn(Collections.singletonList(createServerInfo()));

        testClient.get().uri(VmManagementEndpoint.PATH_LIST_SERVER_INFOS).exchange()
                .expectBody(LIST_SERVER_INFOS_TREF)
                .value(serverInfos -> assertThat(serverInfos).hasSize(1));

        verify(serverInfoResolver, times(1)).resolveAll();
    }

    @Test
    public void testGetAllAgents() {
        when(vmOps.getAgentInfos()).thenReturn(Collections.singletonList(createAgentInfo()));

        testClient.get().uri(VmManagementEndpoint.PATH_LIST_AGENTS).exchange()
                .expectBody(LIST_AGENT_INFO_TREF)
                .value(agentInfos -> assertThat(agentInfos).hasSize(1));

        verify(vmOps, times(1)).getAgentInfos();
    }

    @Test
    public void testGetDetachedAgents() {
        when(vmOps.getDetachedAgentInfos()).thenReturn(Collections.singletonList(createAgentInfo()));

        testClient.get().uri(VmManagementEndpoint.PATH_LIST_AGENTS + "?detached=true").exchange()
                .expectBody(LIST_AGENT_INFO_TREF)
                .value(agentInfos -> assertThat(agentInfos).hasSize(1));

        verify(vmOps, times(1)).getDetachedAgentInfos();
    }

    @Test
    public void testGetJobsOnVMs() {
        when(vmOps.getJobsOnVMs(false, false)).thenReturn(createJobsOnVMs());

        testClient.get().uri(VmManagementEndpoint.PATH_LIST_JOBS_ON_VM).exchange()
                .expectBody(MAP_JOBS_ON_VMS_TREF)
                .value(agentInfos -> assertThat(agentInfos).hasSize(1));

        verify(vmOps, times(1)).getJobsOnVMs(false, false);
    }

    @Test
    public void testGetIdleJobsOnVm() {
        when(vmOps.getJobsOnVMs(true, false)).thenReturn(createJobsOnVMs());

        testClient.get().uri(VmManagementEndpoint.PATH_LIST_JOBS_ON_VM + "?idleOnly=true").exchange()
                .expectBody(MAP_JOBS_ON_VMS_TREF)
                .value(agentInfos -> assertThat(agentInfos).hasSize(1));

        verify(vmOps, times(1)).getJobsOnVMs(true, false);
    }

    private VMOperations.AgentInfo createAgentInfo() {
        return new VMOperations.AgentInfo(
                "testAgent", 10, 8192, 100, 10, Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), "123", Collections.emptyList()
        );
    }

    private ServerInfo createServerInfo() {
        return ServerInfo.newBuilder().build();
    }

    private Map<String, List<VMOperations.JobsOnVMStatus>> createJobsOnVMs() {
        return singletonMap("testAgent", singletonList(
                new VMOperations.JobsOnVMStatus("test.host", "job1")
        ));
    }
}