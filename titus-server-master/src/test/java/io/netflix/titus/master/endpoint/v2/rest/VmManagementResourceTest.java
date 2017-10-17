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

package io.netflix.titus.master.endpoint.v2.rest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.scheduler.VMOperations;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import io.netflix.titus.testkit.junit.jaxrs.HttpTestClient;
import io.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VmManagementResourceTest {

    private static final TypeReference<List<ServerInfo>> LIST_SERVER_INFOS_TREF = new TypeReference<List<ServerInfo>>() {
    };

    private static final TypeReference<List<VMOperations.AgentInfo>> LIST_AGENT_INFO_TREF = new TypeReference<List<VMOperations.AgentInfo>>() {
    };

    private static final TypeReference<Map<String, List<VMOperations.JobsOnVMStatus>>> MAP_JOBS_ON_VMS_TREF =
            new TypeReference<Map<String, List<VMOperations.JobsOnVMStatus>>>() {
            };

    private static ServerInfoResolver serverInfoResolver = mock(ServerInfoResolver.class);
    private static VMOperations vmOps = mock(VMOperations.class);

    private static final VmManagementResource restService = new VmManagementResource(serverInfoResolver, vmOps);

    @ClassRule
    public static final JaxRsServerResource<VmManagementResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static HttpTestClient client;

    @BeforeClass
    public static void setUpClass() throws Exception {
        client = new HttpTestClient(jaxRsServer.getBaseURI() + VmManagementEndpoint.PATH_API_V2_VM);
    }

    @Before
    public void setUp() throws Exception {
        Mockito.reset(serverInfoResolver, vmOps);
    }

    @Test
    public void testGetServerInfos() throws Exception {
        when(serverInfoResolver.resolveAll()).thenReturn(Collections.singletonList(createServerInfo()));

        List<ServerInfo> serverInfos = client.doGET(VmManagementEndpoint.PATH_LIST_SERVER_INFOS, LIST_SERVER_INFOS_TREF);

        verify(serverInfoResolver, times(1)).resolveAll();
        assertThat(serverInfos).hasSize(1);
    }

    @Test
    public void testGetAllAgents() throws Exception {
        when(vmOps.getAgentInfos()).thenReturn(Collections.singletonList(createAgentInfo()));

        List<VMOperations.AgentInfo> agentInfos = client.doGET(VmManagementEndpoint.PATH_LIST_AGENTS, LIST_AGENT_INFO_TREF);

        verify(vmOps, times(1)).getAgentInfos();
        assertThat(agentInfos).hasSize(1);
    }

    @Test
    public void testGetDetachedAgents() throws Exception {
        when(vmOps.getDetachedAgentInfos()).thenReturn(Collections.singletonList(createAgentInfo()));

        Map<String, Object> queryParams = singletonMap("detached", Boolean.TRUE);
        List<VMOperations.AgentInfo> agentInfos = client.doGET(VmManagementEndpoint.PATH_LIST_AGENTS, queryParams, LIST_AGENT_INFO_TREF);

        verify(vmOps, times(1)).getDetachedAgentInfos();
        assertThat(agentInfos).hasSize(1);
    }

    @Test
    public void testGetJobsOnVMs() throws Exception {
        when(vmOps.getJobsOnVMs(false, false)).thenReturn(createJobsOnVMs());

        Map<String, List<VMOperations.JobsOnVMStatus>> jobInfos = client.doGET(VmManagementEndpoint.PATH_LIST_JOBS_ON_VM, MAP_JOBS_ON_VMS_TREF);

        verify(vmOps, times(1)).getJobsOnVMs(false, false);
        assertThat(jobInfos).hasSize(1);
    }

    @Test
    public void testGetIdleJobsOnVm() throws Exception {
        when(vmOps.getJobsOnVMs(true, false)).thenReturn(createJobsOnVMs());

        Map<String, Object> queryParams = singletonMap("idleOnly", Boolean.TRUE);
        Map<String, List<VMOperations.JobsOnVMStatus>> jobInfos = client.doGET(VmManagementEndpoint.PATH_LIST_JOBS_ON_VM, queryParams, MAP_JOBS_ON_VMS_TREF);

        verify(vmOps, times(1)).getJobsOnVMs(true, false);
        assertThat(jobInfos).hasSize(1);
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