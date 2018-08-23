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

import java.net.InetSocketAddress;
import java.util.Optional;

import com.netflix.titus.api.endpoint.v2.rest.representation.LeaderRepresentation;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import com.netflix.titus.master.supervisor.service.MasterMonitor;
import com.netflix.titus.master.mesos.MesosMasterResolver;
import com.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import com.netflix.titus.runtime.endpoint.common.rest.TitusExceptionMapper;
import com.netflix.titus.testkit.junit.jaxrs.HttpTestClient;
import com.netflix.titus.testkit.junit.jaxrs.JaxRsServerResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeaderResourceTest {

    private static final MasterDescription LATEST_MASTER = new MasterDescription(
            "masterHost", "127.0.0.1", 7001, "/api/status", System.currentTimeMillis()
    );

    private static final InetSocketAddress LEADER_ADDRESS = new InetSocketAddress("masterLeader", 5050);
    private static final InetSocketAddress NON_LEADER_ADDRESS = new InetSocketAddress("masterNonLeader", 5050);

    private static MasterMonitor masterMonitor = mock(MasterMonitor.class);

    private static MesosMasterResolver mesosMasterResolver = mock(MesosMasterResolver.class);

    private static final LeaderResource restService = new LeaderResource(masterMonitor, mesosMasterResolver);

    @ClassRule
    public static final JaxRsServerResource<LeaderResource> jaxRsServer = JaxRsServerResource.newBuilder(restService)
            .withProviders(new JsonMessageReaderWriter(), new TitusExceptionMapper())
            .build();

    private static HttpTestClient client;

    @BeforeClass
    public static void setUpClass() throws Exception {
        client = new HttpTestClient(jaxRsServer.getBaseURI());
    }

    @Test
    public void testLeaderReply() throws Exception {
        when(masterMonitor.getLatestLeader()).thenReturn(LATEST_MASTER);
        when(mesosMasterResolver.resolveLeader()).thenReturn(Optional.of(LEADER_ADDRESS));
        when(mesosMasterResolver.resolveMesosAddresses()).thenReturn(asList(LEADER_ADDRESS, NON_LEADER_ADDRESS));

        LeaderRepresentation info = client.doGET("/api/v2/leader", LeaderRepresentation.class);
        assertThat(info.getHostname()).isEqualTo("masterHost");
        assertThat(info.getHostIP()).isEqualTo("127.0.0.1");
        assertThat(info.getApiPort()).isEqualTo(7001);
        assertThat(info.getApiStatusUri()).isEqualTo("/api/status");
        assertThat(info.getMesosLeader()).isEqualTo("masterLeader:5050");
        assertThat(info.getMesosServers()).containsExactly("masterLeader:5050", "masterNonLeader:5050");
    }
}